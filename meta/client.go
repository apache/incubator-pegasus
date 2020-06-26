package meta

import "net/http"

// Client has methods to query the MetaServer for metadata.
type Client interface {
	GetTableInfo() TableInfo
}

// TableInfo is the information of a specified table.
type TableInfo struct {
	AppID          int
	PartitionCount int
}

// NewClient returns a HTTP-based MetaServer Client.
func NewClient(metaAddrs []string) Client {
	return &httpClient{
		metaIpAddresses: metaAddrs,
	}
}

// httpClient queries MetaServer via the HTTP interface.
type httpClient struct {
	metaIpAddresses []string

	hclient http.Client
}

func (*httpClient) GetTableInfo() {
}
