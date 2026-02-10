# Technology Stack

## Languages

- **C**: Primary language (C11 standard required)
- **C++**: Optional, for LLVM JIT support
- **Perl**: Build scripts and testing
- **Python**: Build tools and utilities
- **SQL**: Extension definitions and tests

## Build Systems

PostgreSQL supports two build systems:

### Autoconf (Traditional)
```bash
./configure [options]
make
make install
```

### Meson (Modern, preferred for new development)
```bash
meson setup build [options]
meson compile -C build
meson install -C build
```

Minimum meson version: 0.57.2

## Common Build Commands

### Configuration
```bash
# Autoconf
./configure --prefix=/usr/local/pgsql --enable-debug --enable-cassert

# Meson
meson setup build --prefix=/usr/local/pgsql -Dcassert=true
```

### Building
```bash
# Autoconf
make -j$(nproc)

# Meson
meson compile -C build
```

### Testing
```bash
# Autoconf - regression tests
make check

# Autoconf - all tests
make check-world

# Meson
meson test -C build
```

### Installation
```bash
# Autoconf
make install

# Meson
meson install -C build
```

### Cleaning
```bash
# Autoconf
make clean        # Remove build artifacts
make distclean    # Remove all generated files

# Meson
rm -rf build/
```

## Key Dependencies

### Required
- C compiler (GCC or Clang with C11 support)
- GNU Make (for autoconf builds)
- Flex (>= 2.5.35)
- Bison (>= 2.3)

### Optional
- **ICU**: Unicode and internationalization support
- **OpenSSL**: SSL connections
- **GSSAPI**: Kerberos authentication
- **LDAP**: LDAP authentication
- **PAM**: PAM authentication
- **libxml2**: XML support
- **libxslt**: XSLT support
- **zlib**: Compression
- **LZ4**: LZ4 compression
- **Zstandard**: Zstd compression
- **LLVM** (>= 14): JIT compilation support
- **Tcl**: PL/Tcl procedural language
- **Perl**: PL/Perl procedural language
- **Python**: PL/Python procedural language
- **Readline**: Command-line editing in psql

## Code Generation Tools

- **pgindent**: Code formatting tool (src/tools/pgindent)
- **pg_bsd_indent**: BSD indent variant for PostgreSQL
- **Flex/Bison**: Lexer and parser generators

## Testing Tools

- **pg_regress**: Regression test driver
- **TAP tests**: Perl-based Test Anything Protocol tests (requires IPC::Run)
- **isolation tester**: Concurrent transaction testing
- **prove**: TAP test harness

## Platform Support

- Linux (primary development platform)
- FreeBSD, OpenBSD, NetBSD
- macOS (Darwin)
- Windows (native and Cygwin)
- Solaris

## Compiler Flags

Default optimization: `-O2` (or `/O2` for MSVC)
Debug builds: Add `-g` flag with `--enable-debug`
Assertions: Enable with `--enable-cassert` (autoconf) or `-Dcassert=true` (meson)
