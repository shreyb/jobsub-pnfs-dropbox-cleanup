package main

import (
	"bytes"
	"errors"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"
)

/*
1) Have vault tokens provided by managed tokens?
2) htgettoken for bearer token (set -o flag to save it somewhere else)
3) gfal-ls -l to get list of dirs (NOTE:  Need to use BEARER_TOKEN, not BEARER_TOKEN_FILE)
4) for each dir in (3), output looks like:
```
-bash-4.2$ BEARER_TOKEN=`cat /run/user/10610/bt_u10610` gfal-ls -l  https://fndcadoor.fnal.gov:2880/GM2/resilient/jobsub_stage/5a48ca5816558220979fc6220cb93520b5ef89ed60108c45220327c0de1097f8/
-rwxrwxrwx   0 0     0            50 Sep 26 14:55 bogus_file.out
```

Dir output:
```
drwxrwxrwx   0 0     0             0 Apr  6  2023 bogus_dir
```

5) For each schedd, `condor_q -constraint 'Jobsub_Group=="<experiment>"'  -af PNFS_INPUT_FILES` to get job files (could be comma-separated list)
6) If (3) is too new, discard
7) If (3) is in (5), discard
8) Anything that's left, gfal-ls (3).
9) gfal-rm results from (8)
10) gfal-rm dir in (8)
*/

// "-rwxrwxrwx   0 0     0            50 Sep 26 14:55 bogus_file.out"
// "drwxrwxrwx   0 0     0             0 Apr  6  2022 bogus_dir"

// TODO rename this
var lineRegex = regexp.MustCompile(`((?:\w|-)+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\w+\s+\d+\s+(?:(?:\d+:\d+)|\d+))\s+(.+)`)

var (
	dateWithTimeNoYearLayout string        = "Jan  2 15:04"
	dateWithYearLayout       string        = "Jan 2 2006"
	now                                    = time.Now()
	recentDuration           time.Duration = time.Duration(30 * time.Hour * 24)
)

// FileEntry is a directory file listing
type FileEntry struct {
	filename    string
	created     time.Time
	isDirectory bool
}

type FileAccessor interface {
	getFilesList(source string) ([][]byte, error)
	fileListingToFileEntry(line io.Reader) (FileEntry, error)
	// TODO
	// removeFile(urlOrPath string) error
	// removeDir(urlOrPath string) error
}

// GetDropboxFiles uses a FileAccessor to provide a slice of the files present at the path or URL given by the source string
func GetDropboxFiles(f FileAccessor, source string) ([]FileEntry, error) {
	fileListings, err := f.getFilesList(source)
	if err != nil {
		return nil, err
	}

	fileEntries := make([]FileEntry, 0, len(fileListings))
	for _, listing := range fileListings {
		if entry, err := f.fileListingToFileEntry(bytes.NewReader(listing)); err != nil {
			// TODO log error
			continue
		} else {
			fileEntries = append(fileEntries, entry)
		}
	}

	if len(fileListings) != 0 && len(fileEntries) == 0 {
		return nil, errors.New("there was an error processing the file listings into file entries.  No file entries were generated")
	}
	return fileEntries, nil
}

func scanDropboxLineToFileEntry(line string) (*FileEntry, error) {
	var err error
	lineParts := lineRegex.FindStringSubmatch(line)
	if lineParts == nil {
		return nil, ErrParseLine
	}

	f := &FileEntry{filename: lineParts[7]}
	perms := lineParts[1]
	dateString := lineParts[6]

	f.isDirectory, err = parsePermsToDirectoryFlag(perms)
	if err != nil {
		return nil, ErrParseLine
	}

	f.created, err = parseDateStampToTime(dateString)
	if err != nil {
		return nil, ErrParseLine
	}

	return f, nil
}

func parsePermsToDirectoryFlag(perms string) (bool, error) {
	if len(perms) != 10 {
		return false, ErrMalformedPerms
	}

	validPrefixes := []string{"d", "-"}
	if !slices.Contains(validPrefixes, string(perms[0])) {
		return false, ErrMalformedPerms
	}

	if strings.HasPrefix(perms, "d") {
		return true, nil
	}
	return false, nil
}

func parseDateStampToTime(dateString string) (time.Time, error) {
	var rawDateStamp time.Time
	var err error
	// See if our dateString matches the "Jan  2 15:04 format"
	rawDateStamp, err = time.ParseInLocation(dateWithTimeNoYearLayout, dateString, time.Local)
	if err == nil {
		// We succeeded at parsing this time, so the year will be 0000.  Add the current year on.
		yearDateStamp := rawDateStamp.AddDate(now.Year(), 0, 0)
		if yearDateStamp.After(now) {
			// We're in the future, so subtract a year
			return yearDateStamp.AddDate(-1, 0, 0), nil
		}
		return yearDateStamp, nil
	}
	// The previous parsing attempt failed, so we must be in the "Jan 2 2006" format
	rawDateStamp, err = time.ParseInLocation(dateWithYearLayout, dateString, time.Local)
	if err != nil {
		return time.Time{}, err
	}
	return rawDateStamp, nil
}

func fileIsRecent(f *FileEntry) bool {
	return now.Sub(f.created) < recentDuration
}

type CondorSchedd struct {
}

func (c *CondorSchedd) getDropboxFilesFromJob(j map[string]io.Reader) ([]string, error) {
	attribute := "PNFS_INPUT_FILES"
	val, ok := j[attribute]
	if !ok {
		return nil, ErrMissingJobDropboxFiles
	}

	b := new(strings.Builder)
	_, err := io.Copy(b, val)
	if err != nil {
		return nil, err
	}
	rawSlice := strings.Split((b.String()), ",")
	finalSlice := make([]string, 0, len(rawSlice))

	for _, elt := range rawSlice {
		finalSlice = append(finalSlice, strings.TrimSpace(elt))
	}
	return finalSlice, nil

}

var (
	ErrParseLine              = errors.New("could not parse line")
	ErrMalformedPerms         = errors.New("perms string is malformed")
	ErrMissingJobDropboxFiles = errors.New("required job attribute is missing to get job dropbox files")
)

type JobLister interface {
	queryJobsList(attributes []string, constraint []string) (jobs []map[string][]byte, err error)
	getDropboxFilesFromJob(job map[string]io.Reader) (files []string, err error)
}

func GetActiveFiles(j JobLister, attributes []string, constraints []string) ([]string, error) {
	activeFiles := make([]string, 0)
	// Run Query
	// QueryJobsList(attribute string, ...constraints []string) ([]map[string][]byte, error)
	jobs, err := j.queryJobsList(attributes, constraints)
	if err != nil {
		return activeFiles, err
	}
	for _, job := range jobs {
		readerJob := make(map[string]io.Reader)
		for k, v := range job {
			readerJob[k] = bytes.NewReader(v)
		}

		files, err := j.getDropboxFilesFromJob(readerJob)
		if err != nil {
			// log error
			continue
		}

		activeFiles = append(activeFiles, files...)
	}
	return activeFiles, nil
}
