package executor

import (
	"encoding/json"
	"fmt"

	"github.com/apache/incubator-pegasus/admin-cli/util"
	"github.com/apache/incubator-pegasus/go-client/session"
)

type TableDataVersion struct {
	DataVersion string `json:"data_version"`
}

func QueryTableVersion(client *Client, table string) error {
	version, err := QueryReplicaDataVersion(client, table)
	if err != nil {
		return nil
	}

	// formats into JSON
	outputBytes, _ := json.MarshalIndent(version, "", "  ")
	fmt.Fprintln(client, string(outputBytes))
	return nil
}

func QueryReplicaDataVersion(client *Client, table string) (*TableDataVersion, error) {
	nodes := client.Nodes.GetAllNodes(session.NodeTypeReplica)
	resp, err := client.Meta.QueryConfig(table)
	if err != nil {
		return nil, err
	}

	args := util.Arguments{
		Name:  "app_id",
		Value: string(resp.AppID),
	}
	results := util.BatchCallHTTP(nodes, getTableDataVersion, args)

	var finalVersion string
	var version TableDataVersion
	for _, result := range results {
		if result.Err != nil {
			return nil, result.Err
		}
		err := json.Unmarshal([]byte(result.Resp), &version)
		if err != nil {
			return nil, err
		}

		if finalVersion == "" {
			finalVersion = version.DataVersion
		} else {
			if version.DataVersion == finalVersion {
				continue
			} else {
				return nil, fmt.Errorf("replica versions are not consistent")
			}
		}
	}
	return &version, nil
}

func getTableDataVersion(addr string, args util.Arguments) (string, error) {
	url := fmt.Sprintf("http://%s/replica/data_version?%s=%s", addr, args.Name, args.Value)
	return util.CallHTTPGet(url)
}
