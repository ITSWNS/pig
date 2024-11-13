# PIG(1)                                                         🐷

## NAME

**pig** - A utility to synchronize data between PostgreSQL databases. 

## SYNOPSIS

    pig [OPTIONS] --source=SOURCE --target=TARGET --table=TABLE

## DESCRIPTION

**pig** is a command-line tool written in Go that synchronizes data from a source PostgreSQL database to a target PostgreSQL database. It ensures that the specified table in the target database reflects the data from the source database, based on primary keys and optional conditions.

## OPTIONS

**--source**=CONNECTION\_STRING

:   Connection string for the source PostgreSQL database.

**--target**=CONNECTION\_STRING

:   Connection string for the target PostgreSQL database.

**--table**=TABLE\_NAME

:   The name of the table to synchronize. Supports schema-qualified names (e.g., `public.my_table`).

**--where**="CONDITION"

:   An optional SQL `WHERE` clause to limit the data synchronized from the source.

**--dry-run**

:   Simulate the synchronization process without making changes to the target database.

**--verbose**

:   Enable verbose output, providing detailed information during synchronization.

**--version**

:   Display version information and exit.

## EXIT STATUS

**0**

:   Success.

**Non-zero**

:   Failure.

## EXAMPLES

### Synchronize a table without conditions

Synchronize the entire `users` table from the source to the target database:

```
pig --source="postgres://user:password@source_host:5432/source_db" \
    --target="postgres://user:password@target_host:5432/target_db" \
    --table="users"
```

### Synchronize with a WHERE clause

Synchronize records from the `orders` table where `status` is `pending`:

```
pig --source="postgres://user:password@source_host:5432/source_db" \
    --target="postgres://user:password@target_host:5432/target_db" \
    --table="orders" \
    --where="status = 'pending'"
```

### Perform a dry run

Simulate synchronization without making changes to the target database:

```
pig --source="postgres://user:password@source_host:5432/source_db" \
    --target="postgres://user:password@target_host:5432/target_db" \
    --table="inventory" \
    --dry-run \
    --verbose
```

## AUTHOR

Eliot Alderson

## SEE ALSO

**psql**(1), **pg_dump**(1), **pg_restore**(1)
