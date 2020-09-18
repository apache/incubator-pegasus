package client

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

// MetaClient has methods to query the MetaServer for metadata.
type MetaClient interface {
	// Retrieve all tables in the cluster.
	ListTables() ([]*TableInfo, error)

	// Retrieve all the ReplicaServer nodes in the Pegasus cluster.
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

func (h *httpMetaClient) ListTables() ([]*TableInfo, error) {
	resp, err := h.hclient.Get(fmt.Sprintf("http://%s/meta/apps", h.metaAddr))
	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %s", err.Error())
	}
	body, _ := ioutil.ReadAll(resp.Body)

	var results []*TableInfo
	appMap := gjson.GetBytes(body, "general_info").Map()
	for appIDStr, appInfo := range appMap {
		appID, err := strconv.Atoi(appIDStr)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("unable to parse app ID: \"%s\"", appIDStr))
		}
		partitionCount, found := appInfo.Map()["partition_count"]
		if !found {
			return nil, fmt.Errorf("invalid partition_count from meta server: %s", string(body))
		}
		results = append(results, &TableInfo{
			PartitionCount: int(partitionCount.Int()),
			AppID:          appID,
		})
	}
	return results, nil
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
