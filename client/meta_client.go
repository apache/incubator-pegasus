package client

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/tidwall/gjson"
)

// MetaClient has methods to query the MetaServer for metadata.
type MetaClient interface {
	GetTableInfo(tableName string) (*TableInfo, error)

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

// NewMetaClient returns an HTTP-based MetaServer Client.
func NewMetaClient(metaAddr string) MetaClient {
	if metaAddr == "" {
		panic("the given metaAddr is empty")
	}
	return &httpMetaClient{
		metaAddr: metaAddr,
		hclient:  http.DefaultClient,
	}
}

// httpMetaClient queries MetaServer via the HTTP interface.
type httpMetaClient struct {
	metaAddr string

	hclient *http.Client
}

func (h *httpMetaClient) GetTableInfo(tableName string) (*TableInfo, error) {
	resp, err := h.hclient.Get(fmt.Sprintf("http://%s/meta/app=\"%s\"", h.metaAddr, tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %s", err.Error())
	}
	body, _ := ioutil.ReadAll(resp.Body)

	partitionCount, found := gjson.GetBytes(body, "general").Map()["partition_count"]
	if !found {
		return nil, fmt.Errorf("invalid table info from meta server: %s", string(body))
	}
	appID, found := gjson.GetBytes(body, "general").Map()["app_id"]
	if !found {
		return nil, fmt.Errorf("invalid table info from meta server: %s", string(body))
	}
	return &TableInfo{
		PartitionCount: int(partitionCount.Int()),
		AppID:          int(appID.Int()),
	}, nil
}

func (h *httpMetaClient) ListNodes() ([]*NodeInfo, error) {
	resp, err := h.hclient.Get(fmt.Sprintf("http://%s/meta/nodes?detail", h.metaAddr))
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
