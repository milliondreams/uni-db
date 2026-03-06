//! Test for Issue #18/#150: Poisoned Mutex Handling
//!
//! Verifies that poisoned mutexes don't cause process aborts.
//! Tests that the API returns Result types and handles errors gracefully.

use anyhow::Result;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;
use std::sync::Arc;
use uni_store::runtime::wal::WriteAheadLog;

#[test]
fn test_wal_flushed_lsn_returns_result() -> Result<()> {
    // Issue #18/#150: flushed_lsn() should return Result, not panic on poisoned mutex

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let wal = WriteAheadLog::new(store, Path::from("wal"));

    // Normal case: should work and return Ok
    let lsn = wal.flushed_lsn();
    assert!(
        lsn.is_ok(),
        "flushed_lsn() should return Ok with healthy mutex"
    );
    assert_eq!(lsn.unwrap(), 0, "Initial flushed LSN should be 0");

    // The fix ensures the API returns Result<u64, LockPoisonedError>
    // instead of panicking with .expect(). This prevents process aborts.

    Ok(())
}

/// Test acquire_mutex utility function handles poisoned mutex correctly
#[test]
fn test_acquire_mutex_handles_poison() {
    use std::sync::Mutex;
    use std::thread;
    use uni_common::sync::acquire_mutex;

    let mutex = Arc::new(Mutex::new(42));
    let mutex_clone = mutex.clone();

    // Poison the mutex by panicking while holding the lock
    let handle = thread::spawn(move || {
        let _guard = mutex_clone.lock().unwrap();
        panic!("Intentionally poisoning the mutex");
    });

    // Wait for the thread to panic and poison the mutex
    let _ = handle.join();

    // Verify the mutex is poisoned via standard lock()
    assert!(
        mutex.lock().is_err(),
        "Mutex should be poisoned after panic"
    );

    // Now use acquire_mutex - it should return Err, not panic
    let result = acquire_mutex(&mutex, "test_mutex");
    assert!(
        result.is_err(),
        "acquire_mutex should return Err for poisoned mutex"
    );

    let err = result.unwrap_err();
    assert_eq!(
        err.lock_name, "test_mutex",
        "Error should include lock name"
    );
}
