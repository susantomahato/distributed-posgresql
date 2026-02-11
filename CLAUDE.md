# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

PostgreSQL — an open-source object-relational database management system. Version 19devel. Written primarily in C (C11 standard).

## Build Commands

PostgreSQL supports two build systems: Autoconf (traditional) and Meson (modern).

### Autoconf
```bash
./configure --prefix=/usr/local/pgsql --enable-debug --enable-cassert --enable-tap-tests
make -j$(nproc)
make install
make clean          # remove build artifacts
make distclean      # remove all generated files
```

### Meson
```bash
meson setup build --prefix=/usr/local/pgsql -Dcassert=true
meson compile -C build
meson install -C build
meson test -C build
```

## Testing

### Regression tests (pg_regress)
```bash
make check                          # run regression tests with temp install
make check-world                    # run ALL test suites
make installcheck                   # run against an installed server
make installcheck-parallel          # parallel against installed server
```

### Run specific regression tests
```bash
# From src/test/regress:
make check-tests TESTS="test_name"
```

### TAP tests (require --enable-tap-tests and Perl IPC::Run)
```bash
# Run a specific TAP test:
cd src/bin/pg_rewind && prove t/006_options.pl
```

### Isolation tests (concurrent behavior)
```bash
cd src/test/isolation && make check
```

### Module/extension tests
```bash
cd contrib/some_extension && make check
```

Test output goes to `tmp_check/log/`. Set `PG_TEST_NOCLEAN=1` to preserve test directories. Timeout controlled by `PG_TEST_TIMEOUT_DEFAULT` (default 180s).

## Code Style

- **Indentation:** Tabs, 4-column tab width for C/C++/Perl
- **Line length:** Target 80 columns (not a hard limit)
- **Brace style:** BSD — braces on their own lines
- **Comments:** C-style `/* ... */` only, never `//` (pgindent will replace them)
- **Naming:** lowercase_with_underscores for functions/variables; UPPERCASE for macros; CamelCase for typedefs
- **Include order:** system headers, then `postgres.h` (backend) or `postgres_fe.h` (frontend), then PG headers
- **Function declarations:** always use `extern` keyword
- **Error reporting:** use `ereport()` with `errcode()`, `errmsg()`, and optional `errdetail()`/`errhint()`
- **Memory:** backend uses `palloc()/pfree()` (not malloc); frontend uses `pg_malloc()/pg_free()`

### Formatting tools
```bash
src/tools/pgindent/pgindent .       # format C code
src/tools/pgindent/pgperltidy .     # format Perl code
```

If adding new types, update `src/tools/pgindent/typedefs.list`.

## Architecture

### Source tree layout

- **`src/backend/`** — the server process
  - `parser/` — SQL parsing (flex/bison-generated from `.l`/`.y` files)
  - `optimizer/` — query planner/optimizer
  - `executor/` — query execution engine
  - `access/` — table and index access methods (heap, B-tree, GiST, GIN, BRIN, etc.)
  - `catalog/` — system catalog management
  - `commands/` — SQL command implementations (DDL, etc.)
  - `storage/` — buffer manager, WAL, locking, smgr
  - `replication/` — streaming replication and logical decoding
  - `postmaster/` — process management (postmaster, autovacuum, bgworker)
  - `tcop/` — "traffic cop" — top-level query dispatch
  - `utils/` — caches, memory contexts, GUC, sorting, etc.
  - `rewrite/` — rule system / view expansion
  - `nodes/` — node type infrastructure (plan nodes, expression nodes)
  - `libpq/` — server-side wire protocol handling
- **`src/bin/`** — client tools: `psql`, `pg_dump`, `pg_basebackup`, `pg_rewind`, `pg_upgrade`, etc.
- **`src/interfaces/libpq/`** — the C client library
- **`src/include/`** — all header files (mirrors `src/backend/` structure)
- **`src/common/`** — code shared between frontend and backend
- **`src/port/`** — platform portability layer
- **`src/pl/`** — procedural languages (PL/pgSQL, PL/Perl, PL/Python, PL/Tcl)
- **`src/test/`** — test infrastructure: `regress/`, `isolation/`, `recovery/`, `authentication/`, `ssl/`, `modules/`
- **`contrib/`** — contributed extensions (hstore, pg_stat_statements, pg_trgm, etc.)

### Query processing pipeline

SQL text → **parser** (gram.y) → raw parse tree → **analyzer** (parse_analyze) → Query tree → **rewriter** (rules/views) → rewritten Query → **planner/optimizer** → Plan tree → **executor** → results

### Key conventions

- Node system: all plan/expression types are Node structs with a `NodeTag`. Use `nodeTag()`, `IsA()`, `castNode()` to work with them.
- `Assert()` for internal consistency checks (only active with `--enable-cassert`).
- Catalog changes require updating both `.h` files in `src/include/catalog/` and corresponding `src/backend/catalog/` code.
- System catalog definitions live in header files (e.g., `pg_class.h`, `pg_proc.h`) with `CATALOG()` macros processed by `genbki.pl`.
- Regression test changes: edit `.sql` files in `src/test/regress/sql/` and update expected output in `src/test/regress/expected/`.
