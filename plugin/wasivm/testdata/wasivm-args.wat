;; wasivm-args writes its argument and environment buffers to stdout, then
;; exits 3. Build with: wat2wasm wasivm-args.wat -o wasivm-args.wasm
(module
  (import "wasi_snapshot_preview1" "args_sizes_get" (func $args_sizes_get (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "args_get" (func $args_get (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "environ_sizes_get" (func $environ_sizes_get (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "environ_get" (func $environ_get (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "proc_exit" (func $proc_exit (param i32)))
  (memory (export "memory") 1)
  ;; write writes len bytes at buf to stdout.
  (func $write (param $buf i32) (param $len i32)
    (i32.store (i32.const 8) (local.get $buf))
    (i32.store (i32.const 12) (local.get $len))
    (drop (call $fd_write (i32.const 1) (i32.const 8) (i32.const 1) (i32.const 16))))
  (func (export "_start")
    (drop (call $args_sizes_get (i32.const 0) (i32.const 4)))
    (drop (call $args_get (i32.const 1024) (i32.const 2048)))
    (call $write (i32.const 2048) (i32.load (i32.const 4)))
    (drop (call $environ_sizes_get (i32.const 0) (i32.const 4)))
    (drop (call $environ_get (i32.const 1024) (i32.const 8192)))
    (call $write (i32.const 8192) (i32.load (i32.const 4)))
    (call $proc_exit (i32.const 3))))
