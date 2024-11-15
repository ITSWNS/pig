package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "strings"

    "github.com/jackc/pgx/v4"
    "github.com/spf13/pflag"
)

func main() {
    source := pflag.String("source", "", "Source connection parameters")
    target := pflag.String("target", "", "Target connection parameters")
    table := pflag.String("table", "", "Table to sync")
    where := pflag.String("where", "", "Optional WHERE clause")
    version := pflag.Bool("version", false, "Print version and exit")
    verbose := pflag.Bool("verbose", false, "Enable verbose output")
    dryRun := pflag.Bool("dry-run", false, "Simulate actions without making changes")
    force := pflag.Bool("force", false, "Force upsert of all rows from source into target, ignoring differences")
    help := pflag.Bool("help", false, "Display usage information")

    pflag.Parse()

    if *help {
        fmt.Println("Usage: pig --source=<connection_string> --target=<connection_string> --table=<table_name> [OPTIONS]")
        pflag.PrintDefaults()
        os.Exit(0)
    }

    if *source == "" {
        *source = os.Getenv("PIG_SOURCE")
    }
    if *target == "" {
        *target = os.Getenv("PIG_TARGET")
    }

    if *version {
        fmt.Println("🐷 pig version 0.1.21")
        return
    }

    if *source == "" || *target == "" || *table == "" {
        fmt.Println("Error: --source, --target, and --table are required.")
        pflag.Usage()
        os.Exit(1)
    }

    logger := log.New(os.Stdout, "", log.LstdFlags)
    ctx := context.Background()

    sourceConn, err := pgx.Connect(ctx, *source)
    if err != nil {
        logger.Fatal(err)
    }
    defer sourceConn.Close(ctx)

    targetConn, err := pgx.Connect(ctx, *target)
    if err != nil {
        logger.Fatal(err)
    }
    defer targetConn.Close(ctx)

    err = makeTableSame(ctx, sourceConn, targetConn, *table, *where, *verbose, *dryRun, *force, logger)
    if err != nil {
        logger.Fatal(err)
    }

    if *dryRun {
        logger.Println("Dry-run completed! No changes made to the target. 🐷")
    } else {
        logger.Println("Synchronization completed successfully. 🐷 ")
    }
}

func makeTableSame(ctx context.Context, source, target *pgx.Conn, table, where string, verbose, dryRun, force bool, logger *log.Logger) error {
    schema, tableName := splitSchemaTable(table)

    pkCols, err := getPrimaryKeyColumns(ctx, source, schema, tableName)
    if err != nil {
        return fmt.Errorf("error getting primary key columns: %w", err)
    }
    if len(pkCols) == 0 {
        return fmt.Errorf("table %s has no primary key", table)
    }

    if verbose {
        logger.Printf("Primary key columns: %v", pkCols)
    }

    colNames, err := getColumnNames(ctx, source, schema, tableName)
    if err != nil {
        return fmt.Errorf("error getting column names: %w", err)
    }

    skipColumns := map[string]bool{"search_vector": true}
    filteredColNames := make([]string, 0, len(colNames))
    for _, col := range colNames {
        if !skipColumns[col] {
            filteredColNames = append(filteredColNames, col)
        }
    }

    if verbose && len(filteredColNames) != len(colNames) {
        logger.Printf("Excluded columns: %v", colNames[len(filteredColNames):])
    }

    pkColsStr := joinIdentifiers(pkCols)
    selectCols := joinIdentifiers(filteredColNames)

    sourceQuery := fmt.Sprintf(
        "SELECT %s, md5(row_to_json(t)::text) AS row_hash FROM %s.%s t",
        pkColsStr, quoteIdentifier(schema), quoteIdentifier(tableName),
    )
    if where != "" {
        sourceQuery += " WHERE " + where
    }

    if verbose {
        logger.Printf("Source query: %s", sourceQuery)
    }

    sourceRows, err := source.Query(ctx, sourceQuery)
    if err != nil {
        return fmt.Errorf("error querying source: %w", err)
    }
    defer sourceRows.Close()

    sourceData := make(map[string]string)
    for sourceRows.Next() {
        pkValues := make([]interface{}, len(pkCols))
        scanArgs := make([]interface{}, len(pkCols)+1)
        for i := range pkValues {
            scanArgs[i] = &pkValues[i]
        }
        var rowHash string
        scanArgs[len(pkCols)] = &rowHash

        if err := sourceRows.Scan(scanArgs...); err != nil {
            return fmt.Errorf("error scanning source row: %w", err)
        }

        key := makeKey(pkValues)
        sourceData[key] = rowHash
    }
    if sourceRows.Err() != nil {
        return fmt.Errorf("error reading source rows: %w", sourceRows.Err())
    }

    if verbose {
        logger.Printf("Fetched %d rows from source", len(sourceData))
    }

    targetKeys := make([]string, 0, len(sourceData))
    for key := range sourceData {
        targetKeys = append(targetKeys, key)
    }

    if len(targetKeys) == 0 {
        if verbose {
            logger.Println("No rows to synchronize")
        }
        return nil
    }

    pkPlaceholders := make([]string, len(pkCols))
    for i := range pkCols {
        pkPlaceholders[i] = fmt.Sprintf("%s = ANY($1)", quoteIdentifier(pkCols[i]))
    }
    targetWhereClause := strings.Join(pkPlaceholders, " AND ")

    targetQuery := fmt.Sprintf(
        "SELECT %s, md5(row_to_json(t)::text) AS row_hash FROM %s.%s t WHERE %s",
        pkColsStr, quoteIdentifier(schema), quoteIdentifier(tableName), targetWhereClause,
    )

    if verbose {
        logger.Printf("Target query: %s", targetQuery)
    }

    targetPKValues := make([][]interface{}, len(targetKeys))
    for i, key := range targetKeys {
        targetPKValues[i] = splitKey(key)
    }

    var args []interface{}
    for _, pkValues := range targetPKValues {
        args = append(args, pkValues[0])
    }

    targetRows, err := target.Query(ctx, targetQuery, args)
    if err != nil {
        return fmt.Errorf("error querying target: %w", err)
    }
    defer targetRows.Close()

    targetData := make(map[string]string)
    for targetRows.Next() {
        pkValues := make([]interface{}, len(pkCols))
        scanArgs := make([]interface{}, len(pkCols)+1)
        for i := range pkValues {
            scanArgs[i] = &pkValues[i]
        }
        var rowHash string
        scanArgs[len(pkCols)] = &rowHash

        if err := targetRows.Scan(scanArgs...); err != nil {
            return fmt.Errorf("error scanning target row: %w", err)
        }

        key := makeKey(pkValues)
        targetData[key] = rowHash
    }
    if targetRows.Err() != nil {
        return fmt.Errorf("error reading target rows: %w", targetRows.Err())
    }

    if verbose {
        logger.Printf("Fetched %d rows from target", len(targetData))
    }

    tx, err := target.Begin(ctx)
    if err != nil {
        return fmt.Errorf("error beginning transaction on target: %w", err)
    }
    defer func() {
        if err != nil {
            tx.Rollback(ctx)
        }
    }()

    _, err = tx.Exec(ctx, "SET CONSTRAINTS ALL DEFERRED")
    if err != nil {
        return fmt.Errorf("error deferring constraints on target: %w", err)
    }

    keysToUpsert := make([]string, 0)
    keysToInsert := make([]string, 0)
    keysToDelete := make([]string, 0)

    for key, sourceHash := range sourceData {
        targetHash, exists := targetData[key]
        if !exists {
            keysToInsert = append(keysToInsert, key)
        } else if force || sourceHash != targetHash {
            keysToUpsert = append(keysToUpsert, key)
        }
    }

    // Identify rows to delete (those present in target but missing in source)
    for key := range targetData {
        if _, exists := sourceData[key]; !exists {
            keysToDelete = append(keysToDelete, key)
        }
    }

    if verbose {
        logger.Printf("Rows to insert: %d", len(keysToInsert))
        logger.Printf("Rows to upsert: %d", len(keysToUpsert))
        logger.Printf("Rows to delete: %d", len(keysToDelete))
    }

    placeholders := make([]string, len(filteredColNames))
    for i := range placeholders {
        placeholders[i] = fmt.Sprintf("$%d", i+1)
    }

    updateSet := make([]string, len(filteredColNames))
    for i, col := range filteredColNames {
        updateSet[i] = fmt.Sprintf("%s = EXCLUDED.%s", quoteIdentifier(col), quoteIdentifier(col))
    }

    upsertQuery := fmt.Sprintf(
        "INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
        quoteIdentifier(schema), quoteIdentifier(tableName),
        joinIdentifiers(filteredColNames),
        strings.Join(placeholders, ", "),
        pkColsStr,
        strings.Join(updateSet, ", "),
    )

    if verbose {
        logger.Printf("Upsert query: %s", upsertQuery)
    }

    for _, key := range append(keysToInsert, keysToUpsert...) {
        pkValues := splitKey(key)
        selectQuery := fmt.Sprintf(
            "SELECT %s FROM %s.%s WHERE %s",
            selectCols, quoteIdentifier(schema), quoteIdentifier(tableName), buildWhereClause(pkCols),
        )
        sourceRow := source.QueryRow(ctx, selectQuery, pkValues...)

        columns := make([]interface{}, len(filteredColNames))
        columnPointers := make([]interface{}, len(filteredColNames))
        for i := range columns {
            columnPointers[i] = &columns[i]
        }

        err = sourceRow.Scan(columnPointers...)
        if err != nil {
            return fmt.Errorf("error scanning source row data: %w", err)
        }

        _, err = tx.Exec(ctx, upsertQuery, columns...)
        if err != nil {
            return fmt.Errorf("error upserting row into target: %w", err)
        }
    }

    deleteQuery := fmt.Sprintf(
        "DELETE FROM %s.%s WHERE %s",
        quoteIdentifier(schema), quoteIdentifier(tableName), buildWhereClause(pkCols),
    )

    for _, key := range keysToDelete {
        pkValues := splitKey(key)
        _, err = tx.Exec(ctx, deleteQuery, pkValues...)
        if err != nil {
            return fmt.Errorf("error deleting row from target: %w", err)
        }
    }

    if dryRun {
        err = tx.Rollback(ctx)
        if err != nil {
            return fmt.Errorf("error rolling back transaction: %w", err)
        }
        if verbose {
            logger.Println("Dry-run mode: transaction rolled back.")
        }
    } else {
        err = tx.Commit(ctx)
        if err != nil {
            return fmt.Errorf("error committing transaction: %w", err)
        }
        if verbose {
            logger.Println("Saved changes.")
        }
    }

    return nil
}

func getPrimaryKeyColumns(ctx context.Context, conn *pgx.Conn, schema, table string) ([]string, error) {
    query := `
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_name = $1 AND tc.table_schema = $2
        ORDER BY kcu.ordinal_position
    `
    rows, err := conn.Query(ctx, query, table, schema)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var pkCols []string
    for rows.Next() {
        var colName string
        if err := rows.Scan(&colName); err != nil {
            return nil, err
        }
        pkCols = append(pkCols, colName)
    }
    if rows.Err() != nil {
        return nil, rows.Err()
    }
    return pkCols, nil
}

func getColumnNames(ctx context.Context, conn *pgx.Conn, schema, table string) ([]string, error) {
    query := `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
    `
    rows, err := conn.Query(ctx, query, schema, table)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var colNames []string
    for rows.Next() {
        var colName string
        if err := rows.Scan(&colName); err != nil {
            return nil, err
        }
        colNames = append(colNames, colName)
    }
    if rows.Err() != nil {
        return nil, rows.Err()
    }
    return colNames, nil
}

func buildWhereClause(pkCols []string) string {
    conditions := make([]string, len(pkCols))
    for i, col := range pkCols {
        conditions[i] = fmt.Sprintf("%s = $%d", quoteIdentifier(col), i+1)
    }
    return strings.Join(conditions, " AND ")
}

func makeKey(values []interface{}) string {
    parts := make([]string, len(values))
    for i, v := range values {
        parts[i] = fmt.Sprintf("%v", v)
    }
    return strings.Join(parts, "::")
}

func splitKey(key string) []interface{} {
    parts := strings.Split(key, "::")
    values := make([]interface{}, len(parts))
    for i, part := range parts {
        values[i] = part
    }
    return values
}

func joinIdentifiers(identifiers []string) string {
    for i, id := range identifiers {
        identifiers[i] = quoteIdentifier(id)
    }
    return strings.Join(identifiers, ", ")
}

func quoteIdentifier(identifier string) string {
    identifier = strings.Trim(identifier, `"`)
    return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func splitSchemaTable(table string) (string, string) {
    parts := strings.SplitN(table, ".", 2)
    if len(parts) == 2 {
        return parts[0], parts[1]
    }
    return "public", parts[0]
}
