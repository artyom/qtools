// report-compare prints out common queries collected from multiple databases
package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/artyom/autoflags"
	"github.com/golang/snappy"
)

func main() {
	args := struct {
		Top bool `flag:"top,report only top shard for each query"`
	}{}
	autoflags.Parse(&args)
	if err := run(args.Top, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(top bool, files []string) error {
	if len(files) == 0 {
		return fmt.Errorf("no files to process")
	}
	allQueries := make(map[string]map[key]int)
	for _, name := range files {
		m, err := readReport(name)
		if err != nil {
			return fmt.Errorf("%q: %v", name, err)
		}
		allQueries[name] = m
	}
	type nameCount struct {
		name  string
		count int
	}
	suffix := commonSuffix(files)
	commonQueries := make(map[key][]nameCount)
loop1:
	for k := range allQueries[files[0]] {
		for name, m := range allQueries {
			v, ok := m[k]
			if !ok {
				delete(commonQueries, k)
				continue loop1
			}
			commonQueries[k] = append(commonQueries[k], nameCount{name: name, count: v})
		}
	}
	if len(commonQueries) == 0 {
		return fmt.Errorf("no common queries across all files")
	}
	type reportElement struct {
		header key
		values []nameCount
	}
	var report []reportElement
	for k, vv := range commonQueries {
		sort.Slice(vv, func(i, j int) bool { return vv[i].count > vv[j].count })
		if vv[0].count < 1000 {
			continue
		}
		report = append(report, reportElement{header: k, values: vv})
	}
	sort.Slice(report, func(i, j int) bool { return report[i].values[0].count > report[j].values[0].count })
	for i, elem := range report {
		fmt.Printf("[%s] %s\n", elem.header.schema, elem.header.text)
		for i, v := range elem.values {
			name := strings.TrimSuffix(filepath.Base(v.name), suffix)
			if top && i == 0 {
				switch {
				case len(elem.values) > 1:
					fmt.Printf("\t%d\t%s (%.2f%% ahead)\n", v.count, name,
						(float64(v.count)/float64(elem.values[i+1].count)-1)*100)
				default:
					fmt.Printf("\t%d\t%s\n", v.count, name)
				}
				break
			}
			fmt.Printf("\t%d\t%s\n", v.count, name)
		}
		if i != len(report)-1 {
			fmt.Println()
		}
	}
	return nil
}

func readReport(name string) (map[key]int, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []*queryInfo
	if err := gob.NewDecoder(snappy.NewReader(f)).Decode(&out); err != nil {
		return nil, err
	}
	m := make(map[key]int, len(out))
	for _, qi := range out {
		m[key{schema: qi.Schema, text: qi.Text}] += qi.Count
	}
	return m, nil
}

type queryInfo struct {
	Count    int
	Fraction float64
	Digest   string
	Schema   string
	Text     string
}

type key struct{ schema, text string }

func commonSuffix(l []string) string {
	if len(l) < 2 {
		return ""
	}
	min := reverse(l[0])
	max := min
	for _, s := range l[1:] {
		switch rs := reverse(s); {
		case rs < min:
			min = rs
		case rs > max:
			max = rs
		}
	}
	for i := 0; i < len(min) && i < len(max); i++ {
		if min[i] != max[i] {
			return reverse(min[:i])
		}
	}
	return reverse(min)
}

func reverse(s string) string {
	rs := []rune(s)
	if len(rs) < 2 {
		return s
	}
	for i, j := 0, len(rs)-1; i < j; i, j = i+1, j-1 {
		rs[i], rs[j] = rs[j], rs[i]
	}
	return string(rs)
}
