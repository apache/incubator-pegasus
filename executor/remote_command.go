package executor

import (
	"admin-cli/helper"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/XiaoMi/pegasus-go-client/session"
)

type commandResult struct {
	NodeType session.NodeType
	Address  string
	Command  string
	Result   string
}

func RemoteCommand(client *Client, nodeType session.NodeType, node string, cmd string, args string) error {

	if len(node) != 0 {
		var addr, err = helper.Resolve(node, helper.Host2Addr)
		if err == nil {
			node = addr
		}
	}

	arguments := strings.Split(args, " ")
	if arguments[0] == "" {
		arguments = nil
	}

	var results []*commandResult
	if len(node) == 0 {
		if nodeType == session.NodeTypeMeta {
			resp, err := sendRemoteCommand(client, nodeType, client.MetaAddresses, cmd, arguments)
			if err != nil {
				return err
			}
			results = resp
		} else if nodeType == session.NodeTypeReplica {
			resp, err := sendRemoteCommand(client, nodeType, client.ReplicaAddresses, cmd, arguments)
			if err != nil {
				return err
			}
			results = resp
		} else {
			respMetas, errMeta := sendRemoteCommand(client, session.NodeTypeMeta, client.MetaAddresses, cmd, arguments)
			if errMeta != nil {
				return errMeta
			}
			respReplicas, errReplica := sendRemoteCommand(client, session.NodeTypeReplica, client.ReplicaAddresses, cmd, arguments)
			if errReplica != nil {
				return errReplica
			}
			results = append(results, respMetas...)
			results = append(results, respReplicas...)
		}
	}
	for _, result := range results {
		fmt.Printf("%s[%s][%s] : %s\n", result.Address, result.NodeType, result.Command, result.Result)
	}
	return nil
}

func sendRemoteCommand(client *Client, nodeType session.NodeType, nodes []string, cmd string, args []string) ([]*commandResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	var results []*commandResult
	for _, addr := range nodes {
		remoteClient, errGet := client.GetRemoteCommandClient(addr, nodeType)
		if errGet != nil {
			return nil, errGet
		}
		resp, errCall := remoteClient.Call(ctx, cmd, args)

		var host, errResolve = helper.Resolve(addr, helper.Addr2Host)
		if errResolve == nil {
			addr = fmt.Sprintf("%s[%s]", addr, host)
		}
		if errCall != nil {
			fmt.Printf("Node[%s] send remote command error[%s]\n", addr, errCall)
			continue
		}

		results = append(results, &commandResult{
			NodeType: nodeType,
			Address:  addr,
			Command:  cmd,
			Result:   resp,
		})
	}

	return results, nil
}
