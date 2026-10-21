---
cargo/mitsuro: patch (Fixed)
---

Restore shared server dependencies on Windows, including HTTP, WebSocket, and
push notification clients. Update async-trait to 0.1.92 so generated trait
methods pass strict Clippy checks on the current stable Rust toolchain.
Make Canvas supervisor test fixtures portable to multicall utilities and safe
against concurrent executable-copy races.
