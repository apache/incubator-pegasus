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

func RemoteCommand(client *Client, nodeType session.NodeType, node string, cmd string, args string, enableResolve bool) error {

	if len(node) != 0 && enableResolve {
		var addr, err = helper.Resolve(node, helper.Host2Addr)
		if err != nil {
			return err
		}
		node = addr
	}

	arguments := strings.Split(args, " ")
	if arguments[0] == "" {
		arguments = nil
	}

	var results []*commandResult
	if len(node) == 0 {
		if nodeType == session.NodeTypeMeta {
			resp, err := sendRemoteCommand(client, nodeType, client.MetaAddresses, cmd, arguments, enableResolve)
			if err != nil {
				return err
			}
			results = resp
		} else if nodeType == session.NodeTypeReplica {
			resp, err := sendRemoteCommand(client, nodeType, client.ReplicaAddresses, cmd, arguments, enableResolve)
			if err != nil {
				return err
			}
			results = resp
		} else {
			respMetas, errMeta := sendRemoteCommand(client, session.NodeTypeMeta, client.MetaAddresses, cmd, arguments, enableResolve)
			if errMeta != nil {
				return errMeta
			}
			respReplicas, errReplica := sendRemoteCommand(client, session.NodeTypeReplica, client.ReplicaAddresses, cmd, arguments, enableResolve)
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

func sendRemoteCommand(client *Client, nodeType session.NodeType, nodes []string, cmd string, args []string, enableResolve bool) ([]*commandResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	var results []*commandResult
	for _, addr := range nodes {
		remoteClient, err := client.GetRemoteCommandClient(addr, nodeType)
		if err != nil {
			return nil, err
		}
		resp, err := remoteClient.Call(ctx, cmd, args)
		if err != nil {
			fmt.Printf("Node[%s] send remote command error[%s]\n", addr, err)
			continue
		}

		if enableResolve {
			var host, err = helper.Resolve(addr, helper.Addr2Host)
			if err != nil {
				return nil, err
			}
			addr = host
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
