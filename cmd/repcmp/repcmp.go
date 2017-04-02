// Command repcmp compares new report created with qrep command against old
// report and reports deviations in request proportions exceeding given
// threshold.
package main

import (
	"encoding/gob"
	"fmt"
	"math"
	"os"
	"sync"
	"text/tabwriter"

	"github.com/artyom/autoflags"
	"github.com/golang/snappy"
)

func main() {
	args := &struct {
		Old string  `flag:"old,old report file"`
		New string  `flag:"new,new report file"`
		Dev float64 `flag:"dev,threshold to report, i.e. 0.02 is change for 2% of total requests"`
	}{
		Dev: 0.05,
	}
	autoflags.Parse(args)
	if err := compare(args.Old, args.New, args.Dev); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func compare(oldName, newName string, dev float64) error {
	if dev <= 0 || dev >= 1 {
		return fmt.Errorf("dev should be in (0,1) range")
	}
	oldReport, err := readReport(oldName)
	if err != nil {
		return err
	}
	newReport, err := readReport(newName)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	var printHeader sync.Once
	fn := func(diff float64, q *queryInfo) {
		printHeader.Do(func() { fmt.Fprintf(tw, "Frac(±diff)\tCount\tSchema\tQuery\n") })
		fmt.Fprintf(tw, "%.2f(%+.2f)\t%d\t%s\t%q\n", q.Fraction, diff, q.Count, q.Schema, q.Text)
	}
	oldMap := repToMap(oldReport)
	for _, q1 := range newReport {
		q2, ok := oldMap[q1.key()]
		if !ok {
			if q1.Fraction >= dev {
				fn(q1.Fraction, q1)
			}
			continue
		}
		if diff := q1.Fraction - q2.Fraction; math.Abs(diff) >= dev {
			fn(diff, q1)
		}
	}
	return tw.Flush()
}

func repToMap(s []*queryInfo) map[key]*queryInfo {
	m := make(map[key]*queryInfo)
	for _, qi := range s {
		m[qi.key()] = qi
	}
	return m
}

func readReport(name string) ([]*queryInfo, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []*queryInfo
	if err := gob.NewDecoder(snappy.NewReader(f)).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

type queryInfo struct {
	Count    int
	Fraction float64
	Digest   string
	Schema   string
	Text     string
}

type key struct {
	schema, digest string
}

func (q *queryInfo) key() key { return key{schema: q.Schema, digest: q.Digest} }
