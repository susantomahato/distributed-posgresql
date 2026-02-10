# Coding Conventions

## Code Formatting

### Indentation and Spacing
- Use **tabs** for indentation (not spaces)
- Tab width: **4 columns**
- Each logical indentation level = one additional tab
- Preserve tabs in source files (don't expand to spaces)

### Line Length
- Target: **80 columns** for readability
- Not a hard limit: Don't break long strings arbitrarily just to fit 80 columns
- Use judgment for readability

### Brace Style
- Follow **BSD conventions**
- Curly braces for `if`, `while`, `switch`, etc. go on **their own lines**

```c
if (condition)
{
    do_something();
}
else
{
    do_something_else();
}
```

### Comments
- Use C-style comments: `/* ... */`
- **Never use C++ style** `//` comments (pgindent will replace them)

Multi-line comment style:
```c
/*
 * comment text begins here
 * and continues here
 */
```

Preserve line breaks in indented blocks with dashes:
```c
/*----------
 * comment text begins here
 * and continues here
 *----------
 */
```

Column 1 comments are preserved as-is by pgindent.

## Naming Conventions

### Functions
- Use lowercase with underscores: `function_name()`
- Backend functions often prefixed by subsystem: `heap_insert()`, `btree_search()`

### Variables
- Use lowercase with underscores: `variable_name`
- Keep names descriptive but concise

### Types
- Typedef names often capitalized or use CamelCase
- Struct tags typically lowercase

### Macros and Constants
- Use UPPERCASE with underscores: `MACRO_NAME`

## Error Reporting

### Use ereport() for errors
```c
ereport(ERROR,
        errcode(ERRCODE_DIVISION_BY_ZERO),
        errmsg("division by zero"));
```

### Severity Levels
- `DEBUG1` through `DEBUG5`: Debug messages
- `LOG`: Server log messages
- `INFO`: Information messages
- `NOTICE`: Notice messages
- `WARNING`: Warning messages
- `ERROR`: Error (aborts current transaction)
- `FATAL`: Fatal error (aborts session)
- `PANIC`: Panic (aborts all sessions)

### Error Message Components
- `errcode()`: SQLSTATE error code (see `src/include/utils/errcodes.h`)
- `errmsg()`: Primary error message (required)
- `errdetail()`: Additional detail
- `errhint()`: Hint for fixing the problem
- `errcontext()`: Context information

### Message Style
- Primary messages: One line, describe what went wrong
- Details: Additional technical information
- Hints: Suggestions for resolution (not guaranteed correct)

## Memory Management

### Backend Memory Contexts
- Use palloc()/pfree() instead of malloc()/free()
- Memory contexts automatically cleaned up on error
- Common contexts: `TopMemoryContext`, `CurrentMemoryContext`

### Frontend Code
- Use malloc()/free() or pg_malloc()/pg_free()

## Header Files

### Include Order
1. System headers (`<stdio.h>`, etc.)
2. `postgres.h` (backend) or `postgres_fe.h` (frontend)
3. Other PostgreSQL headers

### Header Guards
Use standard include guards:
```c
#ifndef FILENAME_H
#define FILENAME_H

/* content */

#endif   /* FILENAME_H */
```

## Function Declarations

### Always use `extern` keyword
```c
extern void function_name(int arg);
```

### Static functions
- Declare at top of file or in logical groups
- Use `static` keyword

## Code Organization

### File Structure
1. Copyright header
2. Includes
3. Defines and macros
4. Type definitions
5. Static function declarations
6. Global variables
7. Function implementations

### Function Size
- Keep functions focused and reasonably sized
- Extract complex logic into helper functions

## Platform Compatibility

### Use PostgreSQL Portability Macros
- Don't assume Unix-only features
- Use port layer for platform differences
- Check `src/include/port/` for platform-specific headers

### Integer Types
- Use PostgreSQL types: `int8`, `int16`, `int32`, `int64`
- Use `Size` for memory sizes
- Use `Oid` for object identifiers

## Code Formatting Tools

### pgindent
- Run before submitting patches: `src/tools/pgindent/`
- Reformats code to project standards
- All code gets pgindent'd before releases

### Editor Configuration
- Sample configs in `src/tools/editors/`
- Supports Emacs, vim, xemacs
- Use `.editorconfig` for automatic setup

## Best Practices

### Make New Code Match Existing Code
- Follow the style of surrounding code
- Consistency within a file/module is important

### Use Assertions
- `Assert()` for internal consistency checks (enabled with `--enable-cassert`)
- Not for user-facing error conditions

### Avoid Compiler Warnings
- Code must compile cleanly with `-Wall`
- Fix warnings, don't suppress them unnecessarily

### Write Portable Code
- Test on multiple platforms when possible
- Avoid GNU-specific extensions unless necessary
- Use autoconf/meson feature detection

## SQL and Extension Code

### SQL Style
- Keywords in UPPERCASE
- Identifiers in lowercase
- Indent for readability

### Extension Files
- `.control`: Extension metadata
- `--version.sql`: Upgrade scripts
- Follow versioning conventions: `extension--1.0.sql`, `extension--1.0--1.1.sql`
