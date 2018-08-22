package main

import (
	"log"
	"fmt"
	// "io"
	"os"
	"path"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"github.com/nlevitt/hdfs"
)

type LSifier struct {
	client *hdfs.Client
	long bool
	all bool
	humanReadable bool
	descend bool
	tw *tabwriter.Writer
}

type File struct {
	relPath string // more like path for display
	absPath string
	fileInfo os.FileInfo
}

func ls(paths []string, long, all, humanReadable, dirsPlain, recurse bool) {
	paths, client, err := getClientAndExpandedPaths(paths)
	if err != nil {
		log.Fatal(err)
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
		paths = []string{""}
	}

	var simple []File
	var descend []File
	for _, relOrAbsPath := range paths {
		relPath := relOrAbsPath
		var absPath string
		if strings.HasPrefix(relOrAbsPath, "/") {
			absPath = relOrAbsPath
		} else {
			absPath = path.Join(userDir(client), relPath)
		}
		fileInfo, err := lsifier.client.Stat(absPath)
		if err != nil {
			log.Fatal(err)
		}

		file := File{relPath, absPath, fileInfo}
		if lsifier.descend && fileInfo.IsDir() {
			descend = append(descend, file)
		} else {
			// `ls -d` with no explicit path
			if file.relPath == "" {
				file.relPath = "."
			}
			simple = append(simple, file)
		}
	}

	// after first level, only print dir contents if -R
	lsifier.descend = recurse
	if len(simple) == 0 && len(descend) == 1 {
		for _, dir := range(descend) {
			lsifier.lsDir(dir)
		}
	} else  {
		lsifier.list(simple, descend)
	}
}

func (lsifier *LSifier) list(simple, descend []File) {
	for _, file := range(simple) {
		if lsifier.long {
			lsifier.printLong(file.relPath, file.fileInfo)
		} else {
			fmt.Println(file.relPath)
		}
	}
	if lsifier.long {
		lsifier.tw.Flush()
	}
	for _, dir := range(descend) {
		// // do we want this? java hdfs doesn't do it; bin/ls does,
		// // but format isn't the same, ...
		// fmt.Printf("%s/:\n", dir.relPath)
		lsifier.lsDir(dir)
	}
}

func (lsifier *LSifier) printLong(p string, fileInfo os.FileInfo) {
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
		mode, owner, group, size, date, timeOrYear, p)
}

func (lsifier *LSifier) lsDir(dir File) {
	// special logic for printing . and ..
	if lsifier.all {
		for _, special := range []string{".", ".."} {
			var relPath string
			if dir.relPath == "" || strings.HasSuffix(dir.relPath, "/") {
				relPath = dir.relPath + special
			} else {
				relPath = dir.relPath + "/" + special
			}
			if lsifier.long {
				absPath := path.Join(dir.absPath, special)
				fileInfo, err := lsifier.client.Stat(absPath)
				if err != nil {
					log.Fatal(err)
				}
				lsifier.printLong(relPath, fileInfo)
			} else {
				fmt.Println(relPath)
			}
		}
	}

	// readdir
	dirReader, err := lsifier.client.Open(dir.absPath)
	if err != nil {
		log.Fatalf("lsifier.client.Open(%#v): %v", dir.absPath, err)
	}
	fileInfos, err := dirReader.Readdir(-1)
	if err != nil {
		log.Fatal(err)
	}

	var simple []File
	var descend []File
	for _, fileInfo := range fileInfos {
		if !lsifier.all && strings.HasPrefix(fileInfo.Name(), ".") {
			continue
		}

		relPath := path.Join(dir.relPath, fileInfo.Name())
		absPath := path.Join(dir.absPath, fileInfo.Name())
		file := File{relPath, absPath, fileInfo}

		simple = append(simple, file)
		if lsifier.descend && fileInfo.IsDir() {
			descend = append(descend, file)
		}
	}

	lsifier.list(simple, descend)
}

