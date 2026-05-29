use crate::event_stream::{validate_safe_label, EventStream};
use crate::{EsError, Result, RotationPolicy};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionDescriptor {
    pub namespace: String,
    pub partition_key: String,
    pub path: PathBuf,
}

#[derive(Clone)]
pub struct EventNamespaces {
    inner: Arc<EventNamespacesInner>,
}

struct EventNamespacesInner {
    root: PathBuf,
    rotation_policy: RotationPolicy,
    cache: Mutex<PartitionCache>,
}

struct PartitionCache {
    max_open_stores: usize,
    idle_store_ttl: Duration,
    stores: HashMap<String, CachedStore>,
}

struct CachedStore {
    stream: EventStream,
    last_access: Instant,
}

#[derive(Clone)]
pub struct EventNamespace {
    manager: EventNamespaces,
    name: String,
}

#[derive(Clone)]
pub struct Partition {
    manager: EventNamespaces,
    descriptor: PartitionDescriptor,
}

impl EventNamespaces {
    pub async fn open(root: impl AsRef<Path>, rotation_policy: RotationPolicy) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&root).await?;
        Ok(Self {
            inner: Arc::new(EventNamespacesInner {
                root,
                rotation_policy,
                cache: Mutex::new(PartitionCache {
                    max_open_stores: 50,
                    idle_store_ttl: Duration::from_secs(300),
                    stores: HashMap::new(),
                }),
            }),
        })
    }

    pub fn with_max_open_stores(self, max_open_stores: usize) -> Result<Self> {
        if max_open_stores == 0 {
            return Err(EsError::InvalidPartition(
                "max_open_stores must be greater than zero".to_string(),
            ));
        }

        let mut cache = self.lock_cache()?;
        cache.max_open_stores = max_open_stores;
        cache.evict_to_capacity();
        drop(cache);
        Ok(self)
    }

    pub fn with_idle_store_ttl(self, idle_store_ttl: Duration) -> Result<Self> {
        if idle_store_ttl.is_zero() {
            return Err(EsError::InvalidPartition(
                "idle_store_ttl must be greater than zero".to_string(),
            ));
        }

        let mut cache = self.lock_cache()?;
        cache.idle_store_ttl = idle_store_ttl;
        cache.evict_idle();
        drop(cache);
        Ok(self)
    }

    pub async fn ensure_namespace(&self, namespace: &str) -> Result<EventNamespace> {
        validate_safe_label("namespace", namespace)?;

        let path = self.inner.root.join(namespace);
        tokio::fs::create_dir_all(&path).await?;

        Ok(EventNamespace {
            manager: self.clone(),
            name: namespace.to_string(),
        })
    }

    fn partition_path(&self, namespace: &str, partition_key: &str) -> PathBuf {
        self.inner.root.join(namespace).join(partition_key)
    }

    async fn ensure_partition_exists(
        &self,
        namespace: &str,
        partition_key: &str,
    ) -> Result<Partition> {
        validate_safe_label("namespace", namespace)?;
        validate_safe_label("partition_key", partition_key)?;

        let path = self.partition_path(namespace, partition_key);
        tokio::fs::create_dir_all(&path).await?;

        Ok(Partition {
            manager: self.clone(),
            descriptor: PartitionDescriptor {
                namespace: namespace.to_string(),
                partition_key: partition_key.to_string(),
                path,
            },
        })
    }

    async fn list_partitions(&self, namespace: &str) -> Result<Vec<PartitionDescriptor>> {
        validate_safe_label("namespace", namespace)?;

        let namespace_path = self.inner.root.join(namespace);
        let mut descriptors = Vec::new();
        let mut read_dir = match tokio::fs::read_dir(&namespace_path).await {
            Ok(read_dir) => read_dir,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };

        while let Some(entry) = read_dir.next_entry().await? {
            let file_type = entry.file_type().await?;
            if !file_type.is_dir() {
                continue;
            }

            let Some(partition_key) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if validate_safe_label("partition_key", &partition_key).is_err() {
                continue;
            }

            descriptors.push(PartitionDescriptor {
                namespace: namespace.to_string(),
                partition_key,
                path: entry.path(),
            });
        }

        descriptors.sort_by(|a, b| a.partition_key.cmp(&b.partition_key));
        Ok(descriptors)
    }

    async fn open_cached(&self, descriptor: &PartitionDescriptor) -> Result<EventStream> {
        let cache_key = format!("{}/{}", descriptor.namespace, descriptor.partition_key);
        {
            let mut cache = self.lock_cache()?;
            cache.evict_idle();
            if let Some(cached) = cache.stores.get_mut(&cache_key) {
                cached.last_access = Instant::now();
                return Ok(cached.stream.clone());
            }
        }

        let stream =
            EventStream::open_partitioned(&descriptor.path, self.inner.rotation_policy.clone())
                .await?;

        let mut cache = self.lock_cache()?;
        cache.evict_idle();
        cache.evict_to_capacity();
        cache.stores.insert(
            cache_key,
            CachedStore {
                stream: stream.clone(),
                last_access: Instant::now(),
            },
        );
        Ok(stream)
    }

    fn lock_cache(&self) -> Result<std::sync::MutexGuard<'_, PartitionCache>> {
        self.inner
            .cache
            .lock()
            .map_err(|_| EsError::Migration("partition cache lock poisoned".to_string()))
    }
}

impl EventNamespace {
    pub async fn ensure_partition_exists(&self, partition_key: &str) -> Result<Partition> {
        self.manager
            .ensure_partition_exists(&self.name, partition_key)
            .await
    }

    pub async fn list_partitions(&self) -> Result<Vec<PartitionDescriptor>> {
        self.manager.list_partitions(&self.name).await
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Partition {
    pub async fn open(&self) -> Result<EventStream> {
        if !self.descriptor.path.exists() {
            return Err(EsError::InvalidPath(format!(
                "partition does not exist: {}",
                self.descriptor.path.display()
            )));
        }
        self.manager.open_cached(&self.descriptor).await
    }

    pub fn descriptor(&self) -> &PartitionDescriptor {
        &self.descriptor
    }
}

impl PartitionCache {
    fn evict_idle(&mut self) {
        let ttl = self.idle_store_ttl;
        let now = Instant::now();
        self.stores
            .retain(|_, cached| now.duration_since(cached.last_access) <= ttl);
    }

    fn evict_to_capacity(&mut self) {
        while self.stores.len() >= self.max_open_stores {
            let Some(oldest_key) = self
                .stores
                .iter()
                .min_by_key(|(_, cached)| cached.last_access)
                .map(|(key, _)| key.clone())
            else {
                return;
            };
            self.stores.remove(&oldest_key);
        }
    }
}
