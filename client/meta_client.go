package client

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/tidwall/gjson"
)

// MetaClient has methods to query the MetaServer for metadata.
type MetaClient interface {
	GetTableInfo() (TableInfo, error)

	ListNodes() ([]*NodeInfo, error)
}

// TableInfo is the information of a specified table.
type TableInfo struct {
	AppID          int
	PartitionCount int
}

// NodeInfo is the metadata of a Pegasus ReplicaServer.
type NodeInfo struct {
	Addr string
}

// NewMetaClient returns a HTTP-based MetaServer Client.
func NewMetaClient(metaAddrs []string) MetaClient {
	return &httpMetaClient{
		metaIPAddresses: metaAddrs,
	}
}

// httpMetaClient queries MetaServer via the HTTP interface.
type httpMetaClient struct {
	metaIPAddresses []string

	hclient *http.Client
}

func (*httpMetaClient) GetTableInfo() (TableInfo, error) {
	return TableInfo{}, nil
}

func (h *httpMetaClient) ListNodes() ([]*NodeInfo, error) {
	resp, err := h.hclient.Get(fmt.Sprintf("http://%s/meta/nodes?detail", h.metaIPAddresses[0]))
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %s", err.Error())
	}
	var nodes []*NodeInfo
	body, _ := ioutil.ReadAll(resp.Body)
	result := gjson.GetBytes(body, "details")
	if result.IsObject() {
		result.ForEach(func(key, value gjson.Result) bool {
			nodes = append(nodes, &NodeInfo{Addr: key.String()})
			return true // keep iterating
		})
		return nodes, nil
	}
	return nil, fmt.Errorf("invalid json format in response of /meta/nodes?detail:\n%s", body)
}
