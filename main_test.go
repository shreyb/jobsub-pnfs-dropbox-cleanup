package main

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// adjustAnswerYearIfNeeded is a helper function to ensure that our tests work in the future.
// It is meant to be used with tests that try to use the "current year" by grabbing the Year()
// from time.Now() and using it in a different time.Time object.  This func can be used to wrap
// a time.Date() call to decrement the year if the generated date is in the future.  For example,
// if we have the following (written on 2023-10-08):
//
//		now := time.Now()
//	 t := time.Date(now.Year(), 11, 13, 1, 2, 3, 0, time.Local)
//
// then t is in the future.  If we instead wrap this call in adjustAnswerYearIfNeeded, it will
// ensure that both of the following return dates in the present or past
//
//	t1 := adjustAnswerYearIfNeeded(time.Date(now.Year(), 11, 13, 1, 2, 3, 0, time.Local))
//	t2 := adjustAnswerYearIfNeeded(time.Date(now.Year(), 9, 13, 1, 2, 3, 0, time.Local))
//
// As of this writing, t1 represents a time of 2022-11-13T01:23:00.0 Local Time, and
// t2 represents a time of 2023-09-13T01:23:00.0 Local Time
func adjustAnswerYearIfNeeded(t time.Time) time.Time {
	if t.After(time.Now()) {
		return t.AddDate(-1, 0, 0)
	}
	return t
}

func TestParseDateStampToTime(t *testing.T) {
	type testCase struct {
		description string
		input       string
		output      time.Time
		expectedErr error
	}

	now := time.Now()
	curYear := now.Year()

	createFutureDateStringAndCorrectedTime := func() (string, time.Time) {
		futureDate := now.AddDate(0, 1, 0)
		layout := "Jan 2 15:04"
		return futureDate.Format(layout), now.Truncate(time.Minute).AddDate(-1, 1, 0)
	}

	futureDateString, correctTime := createFutureDateStringAndCorrectedTime()

	testCases := []testCase{
		{
			"Timestamp with time, no year",
			"Sep 26 14:55",
			time.Date(curYear, 9, 26, 14, 55, 0, 0, time.Local),
			nil,
		},
		{
			"Timestamp with time, no year, make sure year gets rewound",
			futureDateString,
			correctTime,
			nil,
		},
		{
			"Timestamp with date, year",
			"Apr  6  2022",
			time.Date(2022, 4, 6, 0, 0, 0, 0, time.Local),
			nil,
		},
		{
			"malformed timestamp",
			"Apr  96  2022",
			time.Time{},
			&time.ParseError{},
		},
		{
			"malformed timestamp 2",
			"boogityboo",
			time.Time{},
			&time.ParseError{},
		},
	}

	for _, test := range testCases {
		t.Run(
			test.description,
			func(t *testing.T) {
				result, err := parseDateStampToTime(test.input)
				if test.expectedErr != nil {
					var err2 *time.ParseError
					assert.ErrorAs(t, err, &err2)
					return
				}
				assert.Equal(t, test.output, result)
			},
		)
	}
}

func TestParsePermsToDirectoryFlag(t *testing.T) {
	type testCase struct {
		input       string
		isDir       bool
		expectedErr error
	}

	testCases := []testCase{
		{
			"-rwxrwxrwx",
			false,
			nil,
		},
		{
			"drwxrwxrwx",
			true,
			nil,
		},
		{
			"boogityboo",
			false,
			ErrMalformedPerms,
		},
	}

	for idx, test := range testCases {
		t.Run(
			fmt.Sprintf("Test%d", idx),
			func(t *testing.T) {
				result, err := parsePermsToDirectoryFlag(test.input)
				if test.expectedErr != nil {
					assert.ErrorIs(t, err, test.expectedErr)
					return
				}
				assert.Equal(t, test.isDir, result)
			},
		)
	}
}

func TestScanDropboxLineToFileEntry(t *testing.T) {
	type testCase struct {
		description       string
		line              string
		expectedFileEntry *FileEntry
	}

	testCases := []testCase{
		{
			"File, no year on datestamp",
			"-rwxrwxrwx   0 0     0            50 Sep 26 14:55 bogus_file.out",
			&FileEntry{
				"bogus_file.out",
				adjustAnswerYearIfNeeded(time.Date(time.Now().Year(), 9, 26, 14, 55, 0, 0, time.Local)),
				false,
			},
		},
		{
			"Directory, no year on datestamp",
			"drwxrwxrwx   0 0     0            50 Sep 26 14:55 bogus_directory",
			&FileEntry{
				"bogus_directory",
				adjustAnswerYearIfNeeded(time.Date(time.Now().Year(), 9, 26, 14, 55, 0, 0, time.Local)),
				true,
			},
		},
		{
			"Timestamp with date, year",
			"drwxrwxrwx   0 0     0             0 Apr  6  2022 bogus_dir",
			&FileEntry{
				"bogus_dir",
				adjustAnswerYearIfNeeded(time.Date(2022, 4, 6, 0, 0, 0, 0, time.Local)),
				true,
			},
		},
	}
	// Should grab file entry for each line

	for _, test := range testCases {
		t.Run(
			test.description,
			func(t *testing.T) {
				entry, _ := scanDropboxLineToFileEntry(test.line)
				assert.Equal(t, test.expectedFileEntry, entry)
			},
		)
	}
}

func TestFileIsRecent(t *testing.T) {
	type testCase struct {
		description string
		f           *FileEntry
		isRecent    bool
	}

	now := time.Now()
	recentDate := now.AddDate(0, 0, -7)
	oldDate := now.AddDate(0, -2, 0)
	reallyOldDate := now.AddDate(-2, 0, 0)

	testCases := []testCase{
		{
			"Recent file",
			&FileEntry{
				"/path/to/recent_file.txt",
				recentDate,
				false,
			},
			true,
		},
		{
			"old file",
			&FileEntry{
				"/path/to/old_file.txt",
				oldDate,
				false,
			},
			false,
		},
		{
			"reallyOld file",
			&FileEntry{
				"/path/to/reallyOld_file.txt",
				reallyOldDate,
				false,
			},
			false,
		},
	}

	for _, test := range testCases {
		t.Run(
			test.description,
			func(t *testing.T) {
				assert.Equal(t, test.isRecent, fileIsRecent(test.f))
			},
		)
	}
}

func TestCondorScheddGetDropboxFilesFromJob(t *testing.T) {
	type testCase struct {
		description   string
		job           map[string]io.Reader
		expectedFiles []string
		expectedErr   error
	}

	testCases := []testCase{
		{
			"One file",
			map[string]io.Reader{"PNFS_INPUT_FILES": strings.NewReader("/path/to/myfile")},
			[]string{"/path/to/myfile"},
			nil,
		},
		{
			"Two files",
			map[string]io.Reader{"PNFS_INPUT_FILES": strings.NewReader("/path/to/myfile,/path/to/myfile2")},
			[]string{"/path/to/myfile", "/path/to/myfile2"},
			nil,
		},
		{
			"Three files, comma-space",
			map[string]io.Reader{"PNFS_INPUT_FILES": strings.NewReader("/path/to/myfile,/path/to/myfile2, /path/to/myfile3")},
			[]string{"/path/to/myfile", "/path/to/myfile2", "/path/to/myfile3"},
			nil,
		},
		{
			"Missing key in job",
			map[string]io.Reader{"PNFS_INPUT_FILES_WRONG": strings.NewReader("/path/to/myfile,/path/to/myfile2, /path/to/myfile3")},
			nil,
			ErrMissingJobDropboxFiles,
		},
	}

	for _, test := range testCases {
		t.Run(
			test.description,
			func(t *testing.T) {
				mySchedd := new(CondorSchedd)
				files, err := mySchedd.getDropboxFilesFromJob(test.job)
				assert.ErrorIs(t, err, test.expectedErr)
				assert.Equal(t, test.expectedFiles, files)
			},
		)
	}
}

type testFileString struct {
	filename string
	isError  bool
}

func newTestJobLister(queryError bool, filestrings ...testFileString) *testJobLister {
	attr := "FILE_ATTRIBUTE"
	g := &testJobLister{
		queryError: queryError,
	}

	g.fileErrors = make(map[string]bool)

	if len(filestrings) == 0 {
		g.files = []testFileString{}
	}

	for _, file := range filestrings {
		job := make(map[string][]byte)

		g.fileErrors[file.filename] = file.isError
		job[attr] = []byte(file.filename)
		g.jobs = append(g.jobs, job)
	}

	return g
}

type testJobLister struct {
	queryError bool
	jobs       []map[string][]byte
	fileErrors map[string]bool
	files      []testFileString
}

func (jl *testJobLister) queryJobsList([]string, []string) ([]map[string][]byte, error) {
	if jl.queryError {
		return nil, errors.New("this is an error")
	}
	return jl.jobs, nil
}

func (jl *testJobLister) getDropboxFilesFromJob(j map[string]io.Reader) ([]string, error) {
	attr := "FILE_ATTRIBUTE"
	if val, ok := j[attr]; ok {
		b := new(strings.Builder)
		io.Copy(b, val)
		if isErr, ok := jl.fileErrors[b.String()]; ok {
			if isErr {
				return nil, errors.New("error!")
			}
		}
		return strings.Split(b.String(), ","), nil
	}
	return nil, errors.New("key missing")
}

func TestGetActiveFiles(t *testing.T) {
	type testCase struct {
		description   string
		jobLister     JobLister
		attributes    []string
		expectedFiles []string
		shouldError   bool
	}

	testCases := []testCase{
		{
			"Good JobLister",
			newTestJobLister(false, testFileString{"/path/to/file1", false}, testFileString{"/path/to/file2", false}),
			nil,
			[]string{"/path/to/file1", "/path/to/file2"},
			false,
		},
		{
			"Empty Good JobLister",
			newTestJobLister(false),
			nil,
			[]string{},
			false,
		},
		{
			"Good JobLister, multiple files per job",
			newTestJobLister(false, testFileString{"/path/to/file1,/path/to/file3", false}, testFileString{"/path/to/file2", false}),
			nil,
			[]string{"/path/to/file1", "/path/to/file3", "/path/to/file2"},
			false,
		},
		{
			"Bad JobLister, bad query, single file",
			newTestJobLister(true, testFileString{"/path/to/file2", false}),
			nil,
			[]string{},
			true,
		},
		{
			"Bad JobLister, bad files extract, single file",
			newTestJobLister(false, testFileString{"/path/to/file2", true}),
			nil,
			[]string{},
			false,
		},
		{
			"Bad JobLister, bad files extract, one good file, one bad",
			newTestJobLister(false, testFileString{"/path/to/file2", true}, testFileString{"/path/to/file1,blahblah", false}),
			nil,
			[]string{"/path/to/file1", "blahblah"},
			false,
		},
	}

	for _, test := range testCases {
		t.Run(
			test.description,
			func(t *testing.T) {
				files, err := GetActiveFiles(test.jobLister, test.attributes, []string{})
				if test.shouldError {
					assert.Error(t, err)
				}
				assert.Equal(t, test.expectedFiles, files)
			},
		)
	}
}

func newTestFileAccessor(files []FileEntry, existsFileListingError bool, errorsByFileEntry []bool) *testFileAccessor {
	return &testFileAccessor{
		fileEntries:            files,
		existsFileListingError: existsFileListingError,
		errorsByFileEntry:      errorsByFileEntry,
	}
}

type testFileAccessor struct {
	fileEntries            []FileEntry
	existsFileListingError bool
	errorsByFileEntry      []bool
}

func (t *testFileAccessor) getFilesList(source string) ([][]byte, error) {
	if t.existsFileListingError {
		return nil, errors.New("some generic file listing error")
	}
	returnSlice := make([][]byte, 0, len(t.fileEntries))
	for _, entry := range t.fileEntries {
		returnSlice = append(returnSlice, []byte(entry.filename))
	}
	return returnSlice, nil
}

func (t *testFileAccessor) fileListingToFileEntry(r io.Reader) (FileEntry, error) {
	var b strings.Builder
	io.Copy(&b, r)
	filename := b.String()
	for idx, entry := range t.fileEntries {
		if entry.filename == filename {
			if t.errorsByFileEntry[idx] {
				return FileEntry{}, errors.New("Fake error that we staged")
			}
			return entry, nil
		}
	}
	return FileEntry{}, errors.New("File not found in testFileAccessor")
}

func TestGetDropboxFiles(t *testing.T) {
	type testCase struct {
		description string
		FileAccessor
		expectedFiles    []FileEntry
		expectedErrorNil bool
	}

	testCases := []testCase{
		{
			"Mix of files and dirs, no errors",
			newTestFileAccessor(
				[]FileEntry{
					{
						"/path/to/foo",
						time.Date(2023, 4, 5, 6, 54, 32, 0, time.Local),
						false,
					},
					{"/path/to/bardir",
						time.Date(2023, 1, 2, 3, 45, 6, 0, time.Local),
						true,
					},
					{
						"/more/sub/dir/paths/to/baz",
						time.Date(2023, 5, 6, 7, 12, 34, 0, time.Local),
						false,
					},
				},
				false,
				[]bool{false, false, false},
			),
			[]FileEntry{
				{
					"/path/to/foo",
					time.Date(2023, 4, 5, 6, 54, 32, 0, time.Local),
					false,
				},
				{"/path/to/bardir",
					time.Date(2023, 1, 2, 3, 45, 6, 0, time.Local),
					true,
				},
				{
					"/more/sub/dir/paths/to/baz",
					time.Date(2023, 5, 6, 7, 12, 34, 0, time.Local),
					false,
				},
			},
			true,
		},
		{
			"Mix of files and dirs, listing error",
			newTestFileAccessor(
				[]FileEntry{
					{
						"/path/to/foo",
						time.Date(2023, 4, 5, 6, 54, 32, 0, time.Local),
						false,
					},
					{"/path/to/bardir",
						time.Date(2023, 1, 2, 3, 45, 6, 0, time.Local),
						true,
					},
					{
						"/more/sub/dir/paths/to/baz",
						time.Date(2023, 5, 6, 7, 12, 34, 0, time.Local),
						false,
					},
				},
				true,
				[]bool{false, true, false},
			),
			nil,
			false,
		},
		{
			"Mix of files and dirs, lines-to-fileEntry errors in some cases",
			newTestFileAccessor(
				[]FileEntry{
					{
						"/path/to/foo",
						time.Date(2023, 4, 5, 6, 54, 32, 0, time.Local),
						false,
					},
					{"/path/to/bardir",
						time.Date(2023, 1, 2, 3, 45, 6, 0, time.Local),
						true,
					},
					{
						"/more/sub/dir/paths/to/baz",
						time.Date(2023, 5, 6, 7, 12, 34, 0, time.Local),
						false,
					},
				},
				false,
				[]bool{false, true, false},
			),
			[]FileEntry{
				{
					"/path/to/foo",
					time.Date(2023, 4, 5, 6, 54, 32, 0, time.Local),
					false,
				},
				{
					"/more/sub/dir/paths/to/baz",
					time.Date(2023, 5, 6, 7, 12, 34, 0, time.Local),
					false,
				},
			},
			true,
		},
		{
			"Mix of files and dirs, lines-to-fileEntry errors in all cases",
			newTestFileAccessor(
				[]FileEntry{
					{
						"/path/to/foo",
						time.Date(2023, 4, 5, 6, 54, 32, 0, time.Local),
						false,
					},
					{"/path/to/bardir",
						time.Date(2023, 1, 2, 3, 45, 6, 0, time.Local),
						true,
					},
					{
						"/more/sub/dir/paths/to/baz",
						time.Date(2023, 5, 6, 7, 12, 34, 0, time.Local),
						false,
					},
				},
				false,
				[]bool{true, true, true},
			),
			nil,
			false,
		},
	}

	for _, test := range testCases {
		t.Run(
			test.description,
			func(t *testing.T) {
				files, err := GetDropboxFiles(test.FileAccessor, "")
				if !test.expectedErrorNil {
					assert.Error(t, err)
				}
				assert.Equal(t, test.expectedFiles, files)
			},
		)
	}
}

// TODO
// FileAccessor interface - arg to GetDropboxFiles() func that returns ([]FileEntry, error).  Constructor to FileAccessor should take pathOrURL string arg
// * Test that checks *condorSchedd.queryJobsList
// * Test that checks *gfalList.getFilesList
// * Test that checks *gfalList.fileListingToFileEntry
