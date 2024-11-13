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

    pflag.Parse()

    if *version {
        fmt.Println("🐷 pig version 0.1.13")
        return
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

    err = makeTableSame(ctx, sourceConn, targetConn, *table, *where, *verbose, logger)
    if err != nil {
        logger.Fatal(err)
    }
}

func makeTableSame(ctx context.Context, source, target *pgx.Conn, table, where string, verbose bool, logger *log.Logger) error {
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

    pkColsStr := joinIdentifiers(pkCols)
    selectCols := joinIdentifiers(colNames)

    // Source query with WHERE clause
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

    // Fetch rows from source
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

    // Fetch target rows matching the primary keys from sourceData
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

    // Build IN clause for target query
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

    // Convert targetKeys to slice of primary key values
    targetPKValues := make([][]interface{}, len(targetKeys))
    for i, key := range targetKeys {
        targetPKValues[i] = splitKey(key)
    }

    // Flatten targetPKValues for query argument
    var args []interface{}
    for _, pkValues := range targetPKValues {
        args = append(args, pkValues[0]) // Assuming single-column primary key
    }

    // Fetch rows from target
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

    // Identify rows to update/insert
    keysToUpsert := make([]string, 0)

    for key, sourceHash := range sourceData {
        targetHash, exists := targetData[key]
        if !exists || sourceHash != targetHash {
            keysToUpsert = append(keysToUpsert, key)
        }
    }

    if verbose {
        logger.Printf("Rows to upsert: %d", len(keysToUpsert))
    }

    // Prepare upsert statement
    placeholders := make([]string, len(colNames))
    for i := range placeholders {
        placeholders[i] = fmt.Sprintf("$%d", i+1)
    }

    updateSet := make([]string, len(colNames))
    for i, col := range colNames {
        updateSet[i] = fmt.Sprintf("%s = EXCLUDED.%s", quoteIdentifier(col), quoteIdentifier(col))
    }

    upsertQuery := fmt.Sprintf(
        "INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
        quoteIdentifier(schema), quoteIdentifier(tableName),
        joinIdentifiers(colNames),
        strings.Join(placeholders, ", "),
        pkColsStr,
        strings.Join(updateSet, ", "),
    )

    if verbose {
        logger.Printf("Upsert query: %s", upsertQuery)
    }

    for _, key := range keysToUpsert {
        pkValues := splitKey(key)
        selectQuery := fmt.Sprintf(
            "SELECT %s FROM %s.%s WHERE %s",
            selectCols, quoteIdentifier(schema), quoteIdentifier(tableName), buildWhereClause(pkCols),
        )
        sourceRow := source.QueryRow(ctx, selectQuery, pkValues...)

        columns := make([]interface{}, len(colNames))
        columnPointers := make([]interface{}, len(colNames))
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

    err = tx.Commit(ctx)
    if err != nil {
        return fmt.Errorf("error committing transaction: %w", err)
    }

    if verbose {
        logger.Println("Synchronization completed successfully. 🐷 ")
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
