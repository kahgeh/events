# Error Types

`EsError` is the public error type for the crate.

Errors fall into three groups:

- caller input errors that should be fixed before retrying
- operational storage errors that may require investigation
- incorrect event versions that need domain-level conflict handling

| Variant | Meaning | Typical response |
| --- | --- | --- |
| `Db` | Turso operation failed | retry if transient, otherwise alert |
| `IncorrectEventVersion` | append expected version did not match the event-stream head | reload state and retry or reject |
| `PayloadTooLarge` | serialized payload exceeds crate limit | reject or reduce payload |
| `Serde` | JSON serialization or deserialization failed | fix payload shape or handler decoding |
| `Uuid` | UUID parsing failed | reject malformed UUID input |
| `Time` | timestamp conversion failed | investigate invalid generated or decoded timestamp |
| `Io` | filesystem operation failed | fix path, permissions, disk, or retry if transient |
| `Migration` | schema migration or row decoding failed | operator intervention |
| `InvalidPartition` | partition state/config/name is invalid | fix caller/configuration/storage state |
| `Cursor` | notification/progress cursor issue | handle at notification layer |
| `InvalidPath` | storage path is invalid | fix configuration |
| `InvalidTableName` | table-name validation failed | fix caller logic; not used by event-stream routing |
| `InvalidVersion` | invalid event-stream version use | fix caller logic |
| `InvalidWorkflowMetadata` | workflow kind/ref shape mismatch | fix caller event construction |
| `InvalidSafeName` | namespace, partition key, or workflow kind is not safe | normalize or reject input |
| `InvalidReadLimit` | bounded read limit is zero or too high | use a smaller positive batch size |
| `CatalogDrift` | events committed but event-stream catalog head update failed | stop writes and investigate |

## Caller Input Errors

These errors usually indicate invalid API use:

- `PayloadTooLarge`
- `InvalidVersion`
- `InvalidWorkflowMetadata`
- `InvalidSafeName`
- `InvalidReadLimit`
- `InvalidPath`
- `InvalidPartition`
- `InvalidTableName`
- `Uuid`
- `Serde`

Do not blindly retry them. Normalize the input, reject the command, or fix the
caller.

## CatalogDrift

`CatalogDrift` means the partition database commit succeeded, but updating the
catalog head failed. Treat it as non-retryable until the affected store has been
inspected. Retrying blindly can duplicate stream versions.

Operational response:

1. Stop writes to the affected `EventStream`.
2. Inspect the catalog and partition files for the committed version.
3. Rebuild or repair the event-stream catalog before appending again.
4. Alert if the underlying cause was disk, permission, or database failure.

## IncorrectEventVersion

`IncorrectEventVersion` reports the expected and actual stream versions. It is scoped to the partition store being appended to.

Typical command-handler handling:

```rust
match stream.append(ExpectedVersion::Exact(seen), events).await {
    Ok(result) => Ok(result),
    Err(EsError::IncorrectEventVersion { expected: _, actual }) => {
        let head = EventStreamVersion::new(actual)?;
        let latest = stream.load_after_version(seen, 100).await?;
        decide_retry_merge_or_reject(head, latest).await
    }
    Err(err) => Err(err),
}
```

`ExpectedVersion::Any` skips the expected-version check, so use it only for events where blind append is domain-correct.

## Storage Errors

`Db`, `Io`, `Migration`, `Time`, and `Uuid` usually mean the store could not
perform or decode an operation. The right response depends on where the error
occurred. Appends require more care than reads because an append may have
partially succeeded before a catalog update failed; `CatalogDrift` is the
explicit signal for that dangerous case.
