package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha512"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
)

type creationContext struct {
	rootArchivePath string
	archive         *tar.Writer
	mapping         map[string]bool
}

func (ctx *creationContext) addDir(dirPath string, archivePath string, dir *os.File, isRoot bool) error {
	entries, err := dir.Readdir(0)
	if err != nil {
		return err
	}

	for _, fi := range entries {
		if isRoot && fi.Name() == ".backing_store" {
			continue
		}

		err = ctx.addEntry(filepath.Join(dirPath, fi.Name()), path.Join(archivePath, fi.Name()), fi)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ctx *creationContext) addEntry(entryPath string, archivePath string, fi os.FileInfo) error {
	f, err := os.OpenFile(entryPath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}

	if fi.Mode()&(os.ModeDir|os.ModeSymlink) == os.ModeDir {
		// Entry is a directory.
		return ctx.addDir(entryPath, archivePath, f, false)
	}

	// Entry is a file. Hash its contents and add a map entry.
	hash := sha512.New()
	_, err = io.Copy(hash, f)
	if err != nil {
		return err
	}

	hashKey := base64.URLEncoding.EncodeToString(hash.Sum(nil))
	backingFileArchivePath := path.Join(ctx.rootArchivePath, ".backing_store", hashKey)

	if _, ok := ctx.mapping[hashKey]; !ok {
		// The hash was not present in the map. Add a new entry to the archive for the backing file.
		_, err = f.Seek(0, os.SEEK_SET)
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(fi, "")
		if err != nil {
			return err
		}

		header.Name = backingFileArchivePath

		err = ctx.archive.WriteHeader(header)
		if err != nil {
			return err
		}

		_, err = io.Copy(ctx.archive, f)
		if err != nil {
			return err
		}

		ctx.mapping[hashKey] = true
	}

	// Add a hard link entry to the archive from the backing file to the archive path.
	header, err := tar.FileInfoHeader(fi, "")
	if err != nil {
		return err
	}

	header.Name = archivePath
	header.Typeflag = tar.TypeLink
	header.Linkname = backingFileArchivePath
	header.Size = 0

	return ctx.archive.WriteHeader(header)
}

func main() {
	flag.Usage = func() {
		_, program := filepath.Split(os.Args[0])
		fmt.Fprintf(os.Stderr, "usage: %s [OPTIONS] [FILE]\n", program)
		flag.PrintDefaults()
	}

	shouldCompress := flag.Bool("compress", false, "compress output using gzip")

	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}

	root, err := filepath.Abs(flag.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		os.Exit(-1)
	}

	f, err := os.OpenFile(root, os.O_RDONLY, os.ModeDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		os.Exit(-1)
	}

	output := io.WriteCloser(os.Stdout)
	if *shouldCompress {
		output = gzip.NewWriter(output)
	}

	_, rootArchivePath := filepath.Split(root)
	ctx := &creationContext{rootArchivePath, tar.NewWriter(output), make(map[string]bool)}

	err = ctx.addDir(root, rootArchivePath, f, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		os.Exit(-1)
	}

	err = ctx.archive.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		os.Exit(-1)
	}

	err = output.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		os.Exit(-1)
	}
}
