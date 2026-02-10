# Project Structure

## Top-Level Directories

- **src/**: All source code
- **contrib/**: Additional supplied modules (extensions)
- **doc/**: Documentation in SGML/XML format
- **config/**: Build configuration scripts and tools

## Source Code Organization (src/)

### Backend (src/backend/)
Core database server code organized by subsystem:

- **access/**: Access methods (heap, index types: btree, hash, gin, gist, brin, etc.)
- **bootstrap/**: Database initialization (initdb internals)
- **catalog/**: System catalog management and definitions
- **commands/**: SQL command implementations (DDL)
- **executor/**: Query executor
- **foreign/**: Foreign data wrapper support
- **jit/**: Just-in-time compilation (LLVM)
- **lib/**: Internal utility libraries
- **libpq/**: Backend side of client/server protocol
- **main/**: Server entry point
- **nodes/**: Parse tree node definitions and manipulation
- **optimizer/**: Query optimizer (planner)
- **parser/**: SQL parser (lexer and grammar)
- **partitioning/**: Table partitioning support
- **postmaster/**: Postmaster (main server process)
- **regex/**: Regular expression engine
- **replication/**: Replication support (logical and physical)
- **rewrite/**: Query rewrite system (rules, views)
- **statistics/**: Extended statistics
- **storage/**: Low-level storage management (buffer, file, lock, page)
- **tcop/**: Traffic cop (query dispatcher)
- **tsearch/**: Full-text search
- **utils/**: Utility functions (adt, cache, error, init, misc, sort, time)

### Client Libraries (src/interfaces/)

- **libpq/**: C client library
- **ecpg/**: Embedded SQL preprocessor for C

### Binary Programs (src/bin/)

- **psql/**: Interactive terminal client
- **pg_dump/**: Database backup utilities
- **pg_basebackup/**: Base backup utility
- **initdb/**: Database cluster initialization
- **pg_ctl/**: Server control utility
- **scripts/**: Various utility scripts

### Procedural Languages (src/pl/)

- **plpgsql/**: PL/pgSQL procedural language
- **plperl/**: PL/Perl procedural language
- **plpython/**: PL/Python procedural language
- **pltcl/**: PL/Tcl procedural language

### Include Files (src/include/)

- **catalog/**: System catalog headers
- **commands/**: Command headers
- **executor/**: Executor headers
- **nodes/**: Node type definitions
- **optimizer/**: Optimizer headers
- **parser/**: Parser headers
- **storage/**: Storage headers
- **utils/**: Utility headers
- **port/**: Platform-specific headers

### Common Code (src/common/)
Code shared between frontend and backend

### Port-Specific Code (src/port/)
Platform compatibility layer

### Frontend Utilities (src/fe_utils/)
Code shared among frontend programs

### Test Suites (src/test/)

- **regress/**: Main regression test suite
- **isolation/**: Isolation/concurrency tests
- **perl/**: Perl testing infrastructure
- **modules/**: Test modules
- **recovery/**: Recovery and replication tests
- **subscription/**: Logical replication tests

## Contrib Modules (contrib/)

Optional extensions and tools:

- **amcheck/**: B-tree index verification
- **bloom/**: Bloom filter index
- **btree_gin/**, **btree_gist/**: Additional index operator classes
- **citext/**: Case-insensitive text type
- **cube/**: Multidimensional cube data type
- **dblink/**: Cross-database queries
- **hstore/**: Key-value store data type
- **ltree/**: Hierarchical tree-like structures
- **pg_stat_statements/**: SQL statement statistics
- **pg_trgm/**: Trigram matching
- **pgcrypto/**: Cryptographic functions
- **postgres_fdw/**: Foreign data wrapper for PostgreSQL
- **uuid-ossp/**: UUID generation

## Configuration Files

- **configure.ac**: Autoconf configuration
- **meson.build**: Meson build configuration
- **GNUmakefile.in**: Top-level makefile template
- **.editorconfig**: Editor configuration for code style

## Important Files

- **COPYRIGHT**: License information
- **HISTORY**: Release notes reference
- **README.md**: Project overview
- **src/DEVELOPERS**: Developer information pointer
- **src/backend/catalog/sql_features.txt**: SQL standard compliance tracking

## File Naming Conventions

- C source files: `.c`
- C headers: `.h`
- Lex files: `.l`
- Yacc/Bison files: `.y`
- SQL scripts: `.sql`
- Expected test output: `.out` (in expected/ directories)
- Test input: `.sql` (in sql/ directories)
