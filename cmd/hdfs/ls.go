package main

import (
	"fmt"
	// "io"
	"os"
	"path"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"github.com/colinmarc/hdfs"
)

type LSifier struct {
	client *hdfs.Client
	long bool
	all bool
	humanReadable bool
	descend bool
	tw *tabwriter.Writer
}

func ls(paths []string, long, all, humanReadable, dirsPlain, recurse bool) {
	paths, client, err := getClientAndExpandedPaths(paths)
	if err != nil {
		fatal(err)
	}

	lsifier := LSifier{
		client: client,
		long: long,
		all: all,
		humanReadable: humanReadable,
		// print dir contents at first level unless -d
		descend: !dirsPlain,
	}
	if long {
		lsifier.tw = tabwriter.NewWriter(os.Stdout, 3, 8, 0, ' ', tabwriter.AlignRight|tabwriter.TabIndent)
	}

	if len(paths) == 0 {
		paths = []string{userDir(client)}
	}

	simple := make(map[string]os.FileInfo)
	descend := make(map[string]os.FileInfo)
	for _, p := range paths {
		fileInfo, err := lsifier.client.Stat(p)
		if err != nil {
			fatal(err)
		}

		if lsifier.descend && fileInfo.IsDir() {
			descend[p] = fileInfo
		}
	}

	// after first level, only print dir contents if -R
	lsifier.descend = recurse
	if len(simple) == 0 && len(descend) == 1 {
		for paff, fileInfo := range(descend) {
			lsifier.lsDir(paff, fileInfo)
		}
	} else  {
		lsifier.list(simple, descend)
	}
}

func (lsifier *LSifier) list(simple, descend map[string]os.FileInfo) {
	if lsifier.long {
		defer lsifier.tw.Flush()
	}
	for paff, fileInfo := range(simple) {
		lsifier.lsFile(paff, fileInfo)
	}
	for paff, fileInfo := range(descend) {
		fmt.Printf("\n%s/:\n", paff)
		lsifier.lsDir(paff, fileInfo)
	}
}

func (lsifier *LSifier) lsFile(paff string, fileInfo os.FileInfo) {
	if lsifier.long {
		lsifier.printLong(paff, fileInfo)
	} else {
		fmt.Println(paff)
	}
}

func (lsifier *LSifier) printLong(paff string, fileInfo os.FileInfo) {
	fi := fileInfo.(*hdfs.FileInfo)
	// mode owner group size date(\w tab) time/year name
	mode := fi.Mode().String()
	owner := fi.Owner()
	group := fi.OwnerGroup()
	size := strconv.FormatInt(fi.Size(), 10)
	if lsifier.humanReadable {
		size = formatBytes(uint64(fi.Size()))
	}

	modtime := fi.ModTime()
	date := modtime.Format("Jan _2")
	var timeOrYear string
	if modtime.Year() == time.Now().Year() {
		timeOrYear = modtime.Format("15:04")
	} else {
		timeOrYear = modtime.Format("2006")
	}

	fmt.Fprintf(lsifier.tw, "%s \t%s \t %s \t %s \t%s \t%s \t%s\n",
		mode, owner, group, size, date, timeOrYear, paff)
}

func (lsifier *LSifier) lsDir(paff string, fileInfo os.FileInfo) {
	// special logic for printing . and ..
	if lsifier.all {
		for _, special := range []string{".", ".."} {
			p := path.Clean(paff) + "/" + special
			if lsifier.long {
				fileInfo, err := lsifier.client.Stat(p)
				if err != nil {
					fatal(err)
				}
				lsifier.printLong(paff, fileInfo)
			} else {
				fmt.Println(p)
			}
		}
	}

	// readdir
	dirReader, err := lsifier.client.Open(paff)
	if err != nil {
		fatal(err)
	}
	files, err := dirReader.Readdir(-1)
	if err != nil {
		fatal(err)
	}

	simple := make(map[string]os.FileInfo)
	descend := make(map[string]os.FileInfo)
	for _, fileInfo := range files {
		if !lsifier.all && strings.HasPrefix(fileInfo.Name(), ".") {
			continue
		}
		p := path.Join(paff, fileInfo.Name())

		simple[p] = fileInfo
		if lsifier.descend && fileInfo.IsDir() {
			descend[p] = fileInfo
		}
	}

	lsifier.list(simple, descend)
}

/*
func printDir(client *hdfs.Client, dir string, long, all, humanReadable bool) {
	dirReader, err := client.Open(dir)
	if err != nil {
		fatal(err)
	}

	var tw *tabwriter.Writer
	if long {
		tw = lsTabWriter()
		defer tw.Flush()
	}

	if all {
		if long {
			dirInfo, err := client.Stat(dir)
			if err != nil {
				fatal(err)
			}

			parentPath := path.Join(dir, "..")
			parentInfo, err := client.Stat(parentPath)
			if err != nil {
				fatal(err)
			}

			printLong(tw, ".", dirInfo, humanReadable)
			printLong(tw, "..", parentInfo, humanReadable)
		} else {
			fmt.Println(".")
			fmt.Println("..")
		}
	}

	var partial []os.FileInfo
	for ; err != io.EOF; partial, err = dirReader.Readdir(100) {
		if err != nil {
			fatal(err)
		}

		printFiles(tw, partial, long, all, humanReadable)
	}

	if long {
		tw.Flush()
	}
}

func printFiles(tw *tabwriter.Writer, files []os.FileInfo, long, all, humanReadable bool) {
	for _, file := range files {
		if !all && strings.HasPrefix(file.Name(), ".") {
			continue
		}

		if long {
			printLong(tw, file.Name(), file, humanReadable)
		} else {
			fmt.Println(file.Name())
		}
	}
}

func printLong(tw *tabwriter.Writer, name string, info os.FileInfo, humanReadable bool) {
	fi := info.(*hdfs.FileInfo)
	// mode owner group size date(\w tab) time/year name
	mode := fi.Mode().String()
	owner := fi.Owner()
	group := fi.OwnerGroup()
	size := strconv.FormatInt(fi.Size(), 10)
	if humanReadable {
		size = formatBytes(uint64(fi.Size()))
	}

	modtime := fi.ModTime()
	date := modtime.Format("Jan _2")
	var timeOrYear string
	if modtime.Year() == time.Now().Year() {
		timeOrYear = modtime.Format("15:04")
	} else {
		timeOrYear = modtime.Format("2006")
	}

	fmt.Fprintf(tw, "%s \t%s \t %s \t %s \t%s \t%s \t%s\n",
		mode, owner, group, size, date, timeOrYear, name)
}

func lsTabWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 3, 8, 0, ' ', tabwriter.AlignRight|tabwriter.TabIndent)
}
*/
