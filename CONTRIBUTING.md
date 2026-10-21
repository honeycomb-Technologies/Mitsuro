# Contributing to Mitsuro

Thanks for helping improve Mitsuro.

## Before you start

- Search existing issues before opening a new one.
- Keep each change focused on one problem.
- Do not include credentials, private logs, customer data, local paths, research
  notes, staging handoffs, or generated build output.
- Read [AGENTS.md](AGENTS.md) before changing the codebase. It contains the
  repository's engineering and release rules.

## Local setup

Build and test the Rust workspace:

```bash
cargo check --workspace
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo fmt --all
```

Validate the shared client:

```bash
cd apps/mobile
bun install --frozen-lockfile
npx tsc --noEmit
npx expo export --platform web
```

Run the local server from the repository root:

```bash
cargo run -p mitsuro
```

Run the Expo client in another terminal:

```bash
cd apps/mobile
npx expo start
```

## Pull requests

A good pull request:

- explains the user-visible problem and outcome;
- lists the checks that were run;
- includes screenshots or recordings for visible UI changes;
- updates documentation when behavior or setup changes;
- preserves compatibility unless a migration is part of the change;
- avoids unrelated formatting or cleanup.

Use Mitsuro for the harness and Hive for durable autonomous work in code,
documentation, tests, packages, routes, storage, and release artifacts. Keep
any prior-identity reader isolated to a tested compatibility boundary.

## Tests that earn their maintenance cost

Test observable behavior through production code, especially ownership, recovery,
streaming, migrations, and failure handling. Do not copy an implementation into a
test and test the copy, or assert source formatting and component spelling as a
substitute for behavior. Reuse fixtures and table cases where they clarify the
contract; remove tests with the retired implementation they only exercised.
Keep a source-level check only when it enforces a named architecture, privacy, or
compatibility boundary that cannot be checked more directly. Client changes run
`deno task test:mobile`, `deno task test:state`, and `bun test packages/api/test tests`.

## Writing changesets

Mitsuro uses [Sampo](https://github.com/bruits/sampo) for version bumps,
changelogs, and `v{version}` tags. Add a changeset on any PR that changes
user-visible CLI, Honey, Hive, mobile, or desktop behavior:

```bash
sampo add -p cargo/mitsuro -b patch -t Added -m "Short user-facing description."
```

Use `minor` for backwards-compatible features and `major` for breaking changes.
`cargo/mitsuro` is fixed to the other 0.9.x product crates. Use
`cargo/mitsuro-hive` for Hive daemon/protocol-only work. Pending files live in
`.sampo/changesets/`.

Do not retag an existing protected tag. Do not publish crates to crates.io.
The cut path is [docs/operations/release.md](docs/operations/release.md):
Version (Sampo) tags, Release binaries uploads archives, Honey installs only
after `sh scripts/release-status.sh` shows the linux archive. Run
`sh scripts/sync-product-version.sh` if Expo drifts from `cargo/mitsuro`.
Sampo may rewrite `Cargo.lock`; restore pins with
`sh scripts/refresh-workspace-lock-versions.sh origin/main`.

## Reporting bugs

Please include:

- Mitsuro version or commit;
- operating system and app surface;
- steps that reproduce the problem;
- expected and actual behavior;
- relevant logs with credentials, tokens, private paths, and personal data
  removed.

Security issues should follow [SECURITY.md](SECURITY.md), not a public issue.
