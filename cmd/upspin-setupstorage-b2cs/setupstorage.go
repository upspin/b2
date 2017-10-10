// Copyright 2017 The Upspin Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The upspin-setupstorage-b2cs command is an external upspin subcommand that
// executes the second step in establishing an upspinserver for backblaze b2.
// Run upspin setupstorage-b2cs -help for more information.
package main // import "b2.upspin.io/cmd/upspin-setupstorage-b2cs"

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	b2api "github.com/kurin/blazer/b2"

	"upspin.io/subcmd"
)

type state struct {
	*subcmd.State
	client *b2api.Client
}

const help = `
Setupstorage-b2cs is the second step in establishing an upspinserver.
It sets up Backblaze B2 cloud storage for your Upspin installation. You may skip this step
if you wish to store Upspin data on your server's local disk.
The first step is 'setupdomain' and the final step is 'setupserver'.

Setupstorage-b2cs creates a Backblaze B2 bucket and updates
the server configuration files in $where/$domain/ to use the specified bucket.

Before running this command, you should ensure you have an B2 account.

If something goes wrong during the setup process, you can run the same command
with the -clean flag. It will attempt to remove any entities previously created
with the same options provided.
`

func main() {
	const name = "setupstorage-b2cs"

	log.SetFlags(0)
	log.SetPrefix("upspin setupstorage-b2cs: ")

	s := &state{
		State: subcmd.NewState(name),
	}
	var err error

	var (
		where       = flag.String("where", filepath.Join(os.Getenv("HOME"), "upspin", "deploy"), "`directory` to store private configuration files")
		domain      = flag.String("domain", "", "domain `name` for this Upspin installation")
		clean       = flag.Bool("clean", false, "deletes all artifacts that would be created using this command")
		b2AccountID = flag.String("account", "", "B2 Cloud Storage account ID")
		b2AppKey    = flag.String("appkey", "", "B2 Cloud Storage application key")
	)

	s.ParseFlags(flag.CommandLine, os.Args[1:], help,
		"setupstorage-b2cs -domain=<name> [-clean] <bucket_name>")
	if flag.NArg() != 1 {
		s.Exitf("a single bucket name must be provided")
	}
	if len(*domain) == 0 {
		s.Exitf("the -domain flag must be provided")
	}
	if len(*b2AccountID) == 0 {
		s.Exitf("the -account flag must be provided")
	}
	if len(*b2AppKey) == 0 {
		s.Exitf("the -appkey flag must be provided")
	}

	s.client, err = b2api.NewClient(context.Background(), *b2AccountID, *b2AppKey)
	if err != nil {
		s.Exitf("unable to create B2 client: %v", err)
	}

	bucketName := flag.Arg(0)
	if *clean {
		s.clean(bucketName)
		s.ExitNow()
	}

	cfgPath := filepath.Join(*where, *domain)
	cfg := s.ReadServerConfig(cfgPath)

	if err := s.createBucket(bucketName); err != nil {
		s.Exitf("unable to create b2cs bucket: %s", err)
	}

	cfg.StoreConfig = []string{
		"backend=B2CS",
		"b2csBucketName=" + bucketName,
		"b2csAccount=" + *b2AccountID,
		"b2csAppKey=" + *b2AppKey,
	}
	s.WriteServerConfig(cfgPath, cfg)

	fmt.Fprintf(os.Stderr, "You should now deploy the upspinserver binary and run 'upspin setupserver'.\n")
	s.ExitNow()
}

func (s *state) createBucket(bucketName string) error {
	_, err := s.client.NewBucket(context.Background(), bucketName, nil)
	return err
}

// clean makes a best-effort attempt at cleaning up entities created by this command.
func (s *state) clean(bucketName string) {
	log.Println("Cleaning up...")

	bucket, err := s.client.Bucket(context.Background(), bucketName)
	if err != nil {
		log.Printf("unable to obtain B2 bucket reference: %v", err)
		return
	}

	if err := bucket.Delete(context.Background()); err != nil {
		log.Printf("unable to delete bucket from b2: %v", err)
	}
}
