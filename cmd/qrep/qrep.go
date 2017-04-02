// Command qrep saves report of queries from MySQL performance_schema database,
// optionally reporting queries exceeding given fraction of all requests.
//
// For comparing two reports against each other to see deviations over time use
// repcmp command.
package main

import (
	"database/sql"
	"encoding/gob"
	"fmt"
	"os"
	"sync"
	"text/tabwriter"

	"github.com/artyom/autoflags"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/snappy"
)

func main() {
	a := &args{
		N: 0.1,
	}
	autoflags.Parse(a)
	if a.DSN == "" {
		a.DSN = os.Getenv("DSN")
	}
	if err := do(a); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func do(a *args) error {
	if a.N < 0 || a.N > 1 {
		return fmt.Errorf("invalid n value, should be in [0,1] range")
	}
	db, err := sql.Open("mysql", a.DSN)
	if err != nil {
		return err
	}
	defer db.Close()
	var queries []*queryInfo
	rows, err := db.Query(queryEvents)
	if err != nil {
		return err
	}
	defer rows.Close()
	var total int
nextRow:
	for rows.Next() {
		var cnt int
		var schema, digest, text string
		if err := rows.Scan(&schema, &cnt, &digest, &text); err != nil {
			return err
		}
		if _, ok := skipDigests[digest]; ok {
			continue nextRow
		}
		total += cnt
		queries = append(queries, &queryInfo{Count: cnt, Schema: schema, Text: text, Digest: digest})

	}
	if err := rows.Err(); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	var printHeader sync.Once
	fn := func(q *queryInfo) {
		printHeader.Do(func() { fmt.Fprintf(tw, "Frac\tCount\tSchema\tQuery\n") })
		fmt.Fprintf(tw, "%.2f\t%d\t%s\t%q\n", q.Fraction, q.Count, q.Schema, q.Text)
	}
	for _, qi := range queries {
		qi.Fraction = float64(qi.Count) / float64(total)
		if a.N == 0 || qi.Fraction < a.N {
			continue
		}
		fn(qi)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if a.File == "" {
		return nil
	}
	if err := dumpToFile(a.File, queries); err != nil {
		return err
	}
	if a.Clear {
		_, err := db.Exec(`truncate table performance_schema.events_statements_summary_by_digest`)
		return err
	}
	return nil
}

func dumpToFile(name string, q []*queryInfo) error {
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	defer f.Close()
	sw := snappy.NewBufferedWriter(f)
	defer sw.Close()
	if err := gob.NewEncoder(sw).Encode(q); err != nil {
		_ = os.Remove(f.Name())
		return err
	}
	if err := sw.Close(); err != nil {
		return err
	}
	return f.Close()
}

type queryInfo struct {
	Count    int
	Fraction float64
	Digest   string
	Schema   string
	Text     string
}

type args struct {
	DSN   string  `flag:"dsn,mysql connection credentials (user:pass@proto(addr)/dbname), also $DSN"`
	File  string  `flag:"file,file to save report to"`
	N     float64 `flag:"n,report query if it attributes to more than n of total number (0 < n <= 1)"`
	Clear bool    `flag:"clear,clear statistics table after dumping report"`
}

const queryEvents = `
select schema_name, count_star, digest, digest_text
from performance_schema.events_statements_summary_by_digest
where schema_name is not NULL and schema_name not in ("mysql", "sys", "tmp") order by count_star desc
`

var skipDigests map[string]struct{}

func init() {
	skipDigests = make(map[string]struct{})
	for _, d := range []string{
		"dca5a150c149dd587e268540040aba81", // SET `autocommit` = ?
		"60a57648fa41632a7fb34c6826d54350", // SET NAMES `utf8mb4`
		"3e44d3b6dd839ce18f1b298bac5ce63f", // COMMIT
		"715f192be9389b835d0c9fb65a12ecd8", // START TRANSACTION
		"d1fa70a5be5d4df14dd4d3c9f1797ef3", // ROLLBACK
		"a8e9da105c1b067941decbba48c66bf4", // SHOW WARNINGS
	} {
		skipDigests[d] = struct{}{}
	}
}
