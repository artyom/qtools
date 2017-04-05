// Command qrep saves report of queries from MySQL performance_schema database,
// optionally reporting queries exceeding given fraction of all requests.
//
// For comparing two reports against each other to see deviations over time use
// repcmp command.
package main

import (
	"bufio"
	"database/sql"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	for rows.Next() {
		var cnt int
		var schema, digest, text string
		if err := rows.Scan(&schema, &cnt, &digest, &text); err != nil {
			return err
		}
		if skipQuery(strings.TrimSpace(text)) {
			continue
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

func skipQuery(s string) bool {
	if strings.HasPrefix(s, "SET ") || strings.HasPrefix(s, "SHOW ") {
		return true
	}
	_, ok := queryBlacklist[s]
	return ok
}

var queryBlacklist map[string]struct{}

func init() {
	queryBlacklist = make(map[string]struct{})
	for _, s := range []string{
		"COMMIT",
		"START TRANSACTION",
		"ROLLBACK",
	} {
		queryBlacklist[s] = struct{}{}
	}
	if f, err := os.Open(filepath.Join(os.Getenv("HOME"), ".qrep-query-blacklist")); err == nil {
		defer f.Close()
		for scanner := bufio.NewScanner(f); scanner.Scan(); {
			s := strings.TrimSpace(scanner.Text())
			if s != "" && !strings.HasPrefix(s, "#") {
				queryBlacklist[s] = struct{}{}
			}
		}
	}
}
