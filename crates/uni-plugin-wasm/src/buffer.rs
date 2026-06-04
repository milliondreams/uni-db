//! `WasmIpcBuffer` — RAII handle for a buffer allocated inside a WASM
//! plugin's linear memory.
//!
//! Specific to the Component Model loader's alloc/copy/free dance for
//! Arrow IPC payloads — kept in this crate (rather than the shared
//! `uni-plugin-wasm-rt`) because the free closure crosses the
//! wasmtime linear-memory boundary.

use std::sync::Arc;

/// RAII handle to a buffer allocated inside a WASM plugin's linear memory.
///
/// Holds the `(ptr, len)` returned by the plugin's `alloc` export; on
/// drop, invokes `free`. Used by the M6 cutover commits to ensure plugin
/// memory is always reclaimed.
pub struct WasmIpcBuffer {
    /// Pointer in the plugin's linear memory.
    pub ptr: u32,
    /// Length in bytes.
    pub len: u32,
    /// Free closure invoked on drop. Wrapped in `Arc<dyn Fn>` so a
    /// pool's instance can hand out buffers without lifetime infection.
    free: Arc<dyn Fn(u32, u32) + Send + Sync>,
}

impl std::fmt::Debug for WasmIpcBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmIpcBuffer")
            .field("ptr", &self.ptr)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl WasmIpcBuffer {
    /// Construct a buffer with the given free closure.
    #[must_use]
    pub fn new(ptr: u32, len: u32, free: Arc<dyn Fn(u32, u32) + Send + Sync>) -> Self {
        Self { ptr, len, free }
    }
}

impl Drop for WasmIpcBuffer {
    fn drop(&mut self) {
        (self.free)(self.ptr, self.len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wasm_ipc_buffer_calls_free_on_drop() {
        let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let c2 = Arc::clone(&counter);
        let free: Arc<dyn Fn(u32, u32) + Send + Sync> = Arc::new(move |_p, _l| {
            c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });
        {
            let _b = WasmIpcBuffer::new(0x1000, 64, free);
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
