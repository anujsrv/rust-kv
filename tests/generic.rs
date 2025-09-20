use kvs::{KvStore, KvsEngine, Result};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use std::fmt::Debug;

// Test with integer keys and values
#[test]
fn test_integer_keys_and_values() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::<u32, i64>::open(temp_dir.path())?;

    store.set(1, 100)?;
    store.set(2, 200)?;
    store.set(3, 300)?;

    assert_eq!(store.get(1)?, Some(100));
    assert_eq!(store.get(2)?, Some(200));
    assert_eq!(store.get(3)?, Some(300));

    // Test overwrite
    store.set(2, 250)?;
    assert_eq!(store.get(2)?, Some(250));

    // Test remove
    assert!(store.remove(2).is_ok());
    assert_eq!(store.get(2)?, None);

    // Test persistence
    drop(store);
    let store = KvStore::<u32, i64>::open(temp_dir.path())?;
    assert_eq!(store.get(1)?, Some(100));
    assert_eq!(store.get(2)?, None);
    assert_eq!(store.get(3)?, Some(300));

    Ok(())
}

// Define a custom struct for testing
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct CustomKey {
    id: u32,
    name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct CustomValue {
    data: String,
    count: u64,
}

// Test with custom struct types
#[test]
fn test_custom_types() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::<CustomKey, CustomValue>::open(temp_dir.path())?;

    let key1 = CustomKey { id: 1, name: "one".to_string() };
    let val1 = CustomValue { data: "value one".to_string(), count: 1 };

    let key2 = CustomKey { id: 2, name: "two".to_string() };
    let val2 = CustomValue { data: "value two".to_string(), count: 2 };

    store.set(key1.clone(), val1.clone())?;
    store.set(key2.clone(), val2.clone())?;

    assert_eq!(store.get(key1.clone())?, Some(val1.clone()));
    assert_eq!(store.get(key2.clone())?, Some(val2.clone()));

    // Test overwrite
    let new_val = CustomValue { data: "updated value".to_string(), count: 42 };
    store.set(key1.clone(), new_val.clone())?;
    assert_eq!(store.get(key1.clone())?, Some(new_val.clone()));

    // Test remove
    assert!(store.remove(key2.clone()).is_ok());
    assert_eq!(store.get(key2.clone())?, None);

    // Test persistence
    drop(store);
    let store = KvStore::<CustomKey, CustomValue>::open(temp_dir.path())?;
    assert_eq!(store.get(key1.clone())?, Some(new_val.clone()));
    assert_eq!(store.get(key2.clone())?, None);

    Ok(())
}

// Test with mixed types
#[test]
fn test_mixed_types() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::<String, u64>::open(temp_dir.path())?;

    store.set("one".to_string(), 1)?;
    store.set("two".to_string(), 2)?;
    store.set("three".to_string(), 3)?;

    assert_eq!(store.get("one".to_string())?, Some(1));
    assert_eq!(store.get("two".to_string())?, Some(2));
    assert_eq!(store.get("three".to_string())?, Some(3));

    // Test overwrite
    store.set("two".to_string(), 22)?;
    assert_eq!(store.get("two".to_string())?, Some(22));

    // Test remove
    assert!(store.remove("two".to_string()).is_ok());
    assert_eq!(store.get("two".to_string())?, None);

    Ok(())
}

