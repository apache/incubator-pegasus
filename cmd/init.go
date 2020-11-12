package cmd

import (
	"github.com/XiaoMi/pegasus-go-client/admin"
)

// globalMetaList is the user-input MetaServer IP:Port addresses
var globalMetaList []string

var pegasusAdminClient admin.Client

// Init all commands to the shell app.
func Init(metaList []string) {
	globalMetaList = metaList

	pegasusAdminClient = admin.NewClient(admin.Config{
		MetaServers: globalMetaList,
	})
}
