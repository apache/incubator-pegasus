package executor

import (
	"context"
	"fmt"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
)

func validateNodeAddress(client *Client, addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.meta.ListNodes(ctx, &admin.ListNodesRequest{
		Status: admin.NodeStatus_NS_INVALID,
	})
	if err != nil {
		return err
	}

	/* TODO(jiashuo1) wait fix the err code
	if resp.Err != base.ERR_OK {
		return fmt.Errorf("Internal server error [%s]", base.ERR_OK)
	}*/

	for _, node := range resp.Infos {
		if node.Address.GetAddress() == addr {
			return nil
		}
	}
	return fmt.Errorf("The cluster doesn't exist the node [%s]", addr)
}

type ResolveType int32

const (
	Host2Addr ResolveType = 0
	Addr2Host ResolveType = 1
)

// node's format, for example, pegasus.onebox.com:34801 or 127.0.0.1:34801
func resolve(node string, resolveType ResolveType) (string, error) {
	splitResult := strings.Split(node, ":")
	if len(splitResult) < 2 {
		return node, fmt.Errorf("Invalid pegasus server node[%s]", node)
	}
	var ip = splitResult[0]
	var port = splitResult[1]

	var nodes []string
	switch resolveType {
	case Host2Addr:
		result, err := net.LookupHost(ip)
		if err != nil {
			return node, err
		}
		nodes = result
	case Addr2Host:
		result, err := net.LookupAddr(ip)
		if err != nil {
			return node, err
		}
		nodes = result
	}

	if len(nodes) == 0 || len(nodes) > 1 {
		return node, fmt.Errorf("Invalid pegasus server node [%s]", node)
	}
	ip = nodes[0]

	// Addr2Host result has suffix `.`, for example, `pegasus.onebox.com.` we need delete the suffix
	if resolveType == Addr2Host {
		ip = strings.TrimSuffix(nodes[0], ".")
	}
	return fmt.Sprintf("%s:%s", ip, port), nil
}

func save2File(client *Client, filePath string) {
	file, _ := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_SYNC|os.O_APPEND, 0755)
	client.Writer = file
}

func str2Gpid(gpid string) (*base.Gpid, error) {
	splitResult := strings.Split(gpid, ".")
	if len(splitResult) < 2 {
		return &base.Gpid{}, fmt.Errorf("Invalid gpid format [%s]", gpid)
	}

	appId, err := strconv.Atoi(splitResult[0])

	if err != nil {
		return &base.Gpid{}, fmt.Errorf("Invalid gpid format [%s]", gpid)
	}

	partitionId, err := strconv.Atoi(splitResult[1])
	if err != nil {
		return &base.Gpid{}, fmt.Errorf("Invalid gpid format [%s]", gpid)
	}

	return &base.Gpid{Appid: int32(appId), PartitionIndex: int32(partitionId)}, nil
}
