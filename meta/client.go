package meta

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/spf13/viper"
	"github.com/tidwall/gjson"
)

// Client has methods to query the MetaServer for metadata.
type Client interface {
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

// NewClient returns a HTTP-based MetaServer Client.
func NewClient() Client {
	metaAddrs := viper.GetStringSlice("meta_servers")
	return &httpClient{
		metaIPAddresses: metaAddrs,
	}
}

// httpClient queries MetaServer via the HTTP interface.
type httpClient struct {
	metaIPAddresses []string

	hclient *http.Client
}

func (*httpClient) GetTableInfo() (TableInfo, error) {
	return TableInfo{}, nil
}

func (h *httpClient) ListNodes() ([]*NodeInfo, error) {
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
	return nil, fmt.Errorf("invalid json format for /meta/nodes?detail")
}
