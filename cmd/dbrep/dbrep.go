// Command dbrep is a wrapper to run qrep + repcmp commands against a set of
// database servers
package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	yaml "gopkg.in/yaml.v2"

	"github.com/artyom/autoflags"
)

func main() {
	now := time.Now().UTC()
	args := &mainArgs{
		N:           0.2,
		Dev:         0.02,
		Dir:         "/tmp",
		File:        "dbhosts.yml",
		DSNtemplate: "user:password@tcp(${HOST}:3306)/",
		New:         now.Format("20060102"),
		Old:         now.AddDate(0, 0, -1).Format("20060102"),
	}
	autoflags.Parse(args)
	if err := do(args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func do(args *mainArgs) error {
	os.Unsetenv("DSN")
	args.Old, args.New = filepath.Base(args.Old), filepath.Base(args.New)
	hosts, err := readMapping(args.File)
	if err != nil {
		return err
	}
	for name, host := range hosts {
		if err := processHost(name, host, args); err != nil {
			return err
		}
	}
	return nil
}

func processHost(name, host string, args *mainArgs) error {
	name = filepath.Base(name)
	dsn := os.Expand(args.DSNtemplate, func(s string) string {
		if s == "HOST" {
			return host
		}
		return ""
	})
	newReport := filepath.Join(args.Dir, name+"."+args.New+".qrep")
	oldReport := filepath.Join(args.Dir, name+"."+args.Old+".qrep")
	cmd1 := exec.Command("qrep", "-file="+newReport, fmt.Sprintf("-n=%v", args.N), fmt.Sprintf("-clear=%v", args.Clear))
	cmd1.Env = append(os.Environ(), "DSN="+dsn)
	cmd2 := exec.Command("repcmp", "-old="+oldReport, "-new="+newReport, fmt.Sprintf("-dev=%v", args.Dev))

	var printHostOnce sync.Once
	for _, cmd := range []*exec.Cmd{cmd1, cmd2} {
		if args.SkipCmp && filepath.Base(cmd.Path) == "repcmp" {
			continue
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			printHostOnce.Do(func() { fmt.Fprintln(os.Stderr, "\nHost:", host) })
			if len(out) > 0 {
				os.Stderr.Write(out)
			}
			return err
		}
		if len(out) > 0 {
			printHostOnce.Do(func() { fmt.Println("\nHost:", host) })
			os.Stdout.Write(out)
		}
	}
	return nil
}

type mainArgs struct {
	N           float64 `flag:"n,-n flag value for qrep command"`
	Dev         float64 `flag:"dev,-dev flag value for repcmp command"`
	Dir         string  `flag:"dir,directory to store reports"`
	Old         string  `flag:"old,part of the old report file name"`
	New         string  `flag:"new,part of the new report file name"`
	File        string  `flag:"map,mapping file with name->hostname pairs"`
	DSNtemplate string  `flag:"tpl,DSN template with $HOST as hostname placeholder"`
	SkipCmp     bool    `flag:"skipcmp,skip repcmp run"`
	Clear       bool    `flag:"clear,-clear flag for qrep command"`
}

func readMapping(name string) (map[string]string, error) {
	b, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string)
	if err := yaml.Unmarshal(b, m); err != nil {
		return nil, err
	}
	return m, nil
}
