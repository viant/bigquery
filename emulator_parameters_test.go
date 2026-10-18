package bigquery

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// This exercises the emulator's positional IN/date binding bug through database/sql.
// It creates no tables and must only target the opt-in local emulator connector.
func TestEmulatorRepeatedPositionalBindings(t *testing.T) {
	dsn := os.Getenv("BIGQUERY_EMULATOR_TEST_DSN")
	if dsn == "" {
		t.Skip("local emulator DSN not configured")
	}
	cfg, err := ParseDSN(dsn)
	if err != nil || !cfg.Emulator {
		t.Fatal("an explicit local emulator connector is required")
	}
	db, err := sql.Open("bigquery", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fields := []string{"CAST(? AS STRING) AS mode", "CAST(? AS STRING) AS date_type", "CASE WHEN ?='left' THEN DATE('2026-09-01') ELSE DATE('2026-09-02') END AS report_date"}
	args := []any{"", "", ""}
	for i := 0; i < 14; i++ {
		fields = append(fields, fmt.Sprintf("SUM(IF(TRUE,IF(?='alpha',1,2),0)) AS metric%d", i))
		args = append(args, "")
	}
	query := "SELECT sample.* FROM (SELECT " + strings.Join(fields, ",") + " FROM (SELECT 1 AS id) records WHERE ? IN ('alpha','beta') AND ? IN ('left','right') AND ((?='left' AND DATE('2026-09-02') BETWEEN DATE(?) AND DATE(?)) OR (?='right' AND DATE('2026-09-02') BETWEEN DATE(?) AND DATE(?))) GROUP BY mode,date_type,report_date) sample WHERE 1=0"
	args = append(args, "", "", "", "2026-09-01", "2026-09-03", "", "2026-09-01", "2026-09-03")
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil || len(columns) != 17 {
		t.Fatalf("metadata columns=%v error=%v", columns, err)
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("metadata query returned rows or error: %v", rows.Err())
	}
	// Real values must retain the same argument order after metadata discovery.
	for i := range args {
		args[i] = "alpha"
	}
	args[1] = "left"
	args[2] = "left"
	args[18] = "left"
	args[19] = "left"
	args[22] = "right"
	for _, i := range []int{20, 23} {
		args[i] = "2026-09-01"
	}
	for _, i := range []int{21, 24} {
		args[i] = "2026-09-03"
	}
	query = strings.TrimSuffix(query, " WHERE 1=0")
	rows, err = db.QueryContext(ctx, query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected one row: %v", rows.Err())
	}
	values := make([]any, 17)
	dest := make([]any, 17)
	for i := range dest {
		dest[i] = &values[i]
	}
	if err = rows.Scan(dest...); err != nil {
		t.Fatal(err)
	}
	if values[0] != "alpha" || values[1] != "left" || fmt.Sprint(values[3]) != "1" {
		t.Fatalf("wrong parameter binding: %v", values)
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("unexpected extra row/error: %v", rows.Err())
	}
}
