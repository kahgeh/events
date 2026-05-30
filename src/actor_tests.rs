use super::*;

#[test]
fn test_actor_type_as_str() {
    assert_eq!(ActorType::User.as_str(), "user");
    assert_eq!(ActorType::System.as_str(), "system");
    assert_eq!(ActorType::Admin.as_str(), "admin");
    assert_eq!(ActorType::ApiKey.as_str(), "api_key");
}

#[test]
fn test_actor_type_from_str() {
    assert_eq!("user".parse::<ActorType>().unwrap(), ActorType::User);
    assert_eq!("system".parse::<ActorType>().unwrap(), ActorType::System);
    assert_eq!("admin".parse::<ActorType>().unwrap(), ActorType::Admin);
    assert_eq!("api_key".parse::<ActorType>().unwrap(), ActorType::ApiKey);
}

#[test]
fn test_actor_type_from_str_invalid() {
    assert!("invalid".parse::<ActorType>().is_err());
    assert!("USER".parse::<ActorType>().is_err());
}

#[test]
fn test_actor_type_display() {
    assert_eq!(format!("{}", ActorType::User), "user");
    assert_eq!(format!("{}", ActorType::System), "system");
}

#[test]
fn test_actor_type_serde() {
    let user = ActorType::User;
    let json = serde_json::to_string(&user).unwrap();
    assert_eq!(json, "\"user\"");

    let parsed: ActorType = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, ActorType::User);
}
