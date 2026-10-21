---
cargo/mitsuro: patch (Changed)
cargo/mitsuro-hive: patch (Fixed)
---

Remove retired desktop implementations and unreachable runtime code, reduce
unused terminal dependencies, and retain legacy session recovery through the
shared child executor. Stop advertising an unavailable built-in terminal plugin.
Serialize the schedule schema update so simultaneous server and Hive startup
cannot race to add the same column.
