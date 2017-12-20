// Copyright 2017 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package b2cs

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"upspin.io/cloud/storage"
	"upspin.io/log"
	"upspin.io/upspin"
)

const defaultTestBucketName = "upspin-test-scratch"

var (
	client        storage.Storage
	testDataStr   = fmt.Sprintf("This is test at %v", time.Now())
	testData      = []byte(testDataStr)
	fileName      = fmt.Sprintf("test-file-%d", time.Now().Second())
	testBucket    = flag.String("test_bucket", defaultTestBucketName, "bucket name to use for testing")
	testAccountID = flag.String("account", "", "B2 Cloud Storage account ID")
	testAppKey    = flag.String("appkey", "", "B2 Cloud Storage application key")
	useB2CS       = flag.Bool("use_b2cs", false, "enable to run b2cs tests; requires Backblaze credentials")

	objectContents = []byte(fmt.Sprintf("This is test at %v", time.Now()))
)

func TestListingEmptyContainer(t *testing.T) {
	l := client.(*b2csImpl)
	refs, nextToken, err := l.List("")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 {
		t.Errorf("List returned %d refs, want 0", len(refs))
	}
	if nextToken != "" {
		t.Errorf("List returned token %q, want empty string", nextToken)
	}
}

func TestListingWithPagination(t *testing.T) {
	putRefs := make([]string, 10)
	for i := 0; i < 10; i++ {
		ref := fmt.Sprintf("ref%d", i)
		putRefs[i] = ref
		if err := client.Put(ref, objectContents); err != nil {
			t.Fatal(err)
		}
	}

	refs, callCount, err := getAllRefs(3, len(putRefs))
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != len(putRefs) {
		t.Errorf("Listed %d refs, want %d", len(refs), len(putRefs))
	}
	if want := 4; callCount != want {
		t.Errorf("List split into %d pages, want %d", callCount, want)
	}
}

func getAllRefs(perPage int, maxCalls int) (allRefs []upspin.ListRefsItem, callCount int, err error) {
	l := client.(*b2csImpl)
	var token string

	oldMax := maxResults
	maxResults = perPage
	defer func() { maxResults = oldMax }()

	for callCount < maxCalls {
		var refs []upspin.ListRefsItem
		refs, token, err = l.List(token)
		callCount++
		if err != nil {
			break
		}
		allRefs = append(allRefs, refs...)
		if token == "" {
			break
		}
	}
	return
}

// The tests run against the live B2 Cloud Storage, not against a mocked B2
// service. Because of that, credentials for an existing B2 account need to be
// supplied with command-line flags to "go test". The test bucket is deleted
// when the tests ran.
func TestPutGetAndDownload(t *testing.T) {
	err := client.Put(fileName, testData)
	if err != nil {
		t.Fatalf("Can't put: %v", err)
	}
	data, err := client.Download(fileName)
	if err != nil {
		t.Fatalf("Can't Download: %v", err)
	}
	if string(data) != testDataStr {
		t.Errorf("Expected %q got %q", testDataStr, string(data))
	}
	// Check that Download yields the same data
	bytes, err := client.Download(fileName)
	if err != nil {
		t.Fatal(err)
	}
	if string(bytes) != testDataStr {
		t.Errorf("Expected %q got %q", testDataStr, string(bytes))
	}
}

func TestDelete(t *testing.T) {
	// Use a dedicated fileName for the deletion test, otherwise
	// it will simply be second version of the same file as for
	// TestPutGetAndDownload. Delete would then only erase one version and
	// Download would still find the file.
	fileNameDelete := "deletiontest-" + fileName
	err := client.Put(fileNameDelete, testData)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Delete(fileNameDelete)
	if err != nil {
		t.Fatalf("Expected no errors, got %v", err)
	}
	// Test the side effect after Delete.
	_, err = client.Download(fileNameDelete)
	if err == nil {
		t.Fatal("Expected an error, but got none")
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	if !*useB2CS {
		log.Printf(`
cloud/storage/b2cs: skipping test as it requires B2 Cloud Storage access. To
enable this test, provide an account and key with flags -account and -appkey,
respectively, to upload to an B2 Cloud Storage bucket named by flag -test_bucket
and then set this test's flag -use_b2cs.
`)
		os.Exit(0)
	}
	// Create client that writes to test bucket.
	var err error
	client, err = storage.Dial("B2CS",
		storage.WithKeyValue("b2csBucketName", *testBucket),
		storage.WithKeyValue("b2csAccount", *testAccountID),
		storage.WithKeyValue("b2csAppKey", *testAppKey))
	if err != nil {
		log.Fatalf("cloud/storage/b2cs: couldn't set up client: %v", err)
	}
	code := m.Run()
	// Clean up.
	if err := client.(*b2csImpl).deleteBucket(); err != nil {
		log.Printf("cloud/storage/b2cs: deleteBucket failed: %v", err)
	}
	os.Exit(code)
}
