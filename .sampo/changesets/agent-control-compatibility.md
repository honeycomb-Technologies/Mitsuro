---
cargo/mitsuro: patch (Fixed)
---

Keep Code sessions usable when clients and servers upgrade separately. New
clients recognize the preceding server's absent control route and SPA fallback;
old clients share a bounded compatibility controller until a modern client
takes control. Preserve active-controller fencing and session ownership checks.
