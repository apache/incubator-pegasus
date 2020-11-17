package cmd

import (
	"admin-cli/executor"
	"os"
)

// globalMetaList is the user-input MetaServer IP:Port addresses
var globalMetaList []string

var pegasusClient *executor.Client

// Init all commands to the shell app.
func Init(metaList []string) {
	globalMetaList = metaList

	pegasusClient = executor.NewClient(os.Stdout, globalMetaList)
}
