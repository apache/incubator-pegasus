package tablemigrator

import (
	"fmt"
	"time"

	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits"
	"github.com/apache/incubator-pegasus/admin-cli/util"
	"github.com/apache/incubator-pegasus/go-client/session"
	"github.com/pegasus-kv/collector/aggregate"
)

var pendingMutationThreshold = 100000.0

func MigrateTable(client *executor.Client, table string, metaProxyZkAddrs string, metaProxyZkRoot string, targetCluster string, targetAddrs string, threshold float64) error {
	pendingMutationThreshold = threshold
	toolkits.LogInfo(fmt.Sprintf("set pendingMutationThreshold = %f means if the pending less the value will "+
		"reject all write and ready to switch cluster", pendingMutationThreshold))
	//1. check data version
	toolkits.LogInfo("check data version")
	version, err := executor.QueryReplicaDataVersion(client, table)
	if err != nil {
		return err
	}
	if version.DataVersion != "1" {
		return fmt.Errorf("not support migrate table with data_version = %s by duplication", version.DataVersion)
	}

	//2. create table duplication
	toolkits.LogInfo("create table duplication")
	err = executor.AddDuplication(client, table, targetCluster, true)
	if err != nil {
		return err
	}

	//3. check un-confirm decree if less 5k
	toolkits.LogInfo(fmt.Sprintf("check un-confirm decree if less %f", pendingMutationThreshold))
	nodes := client.Nodes.GetAllNodes(session.NodeTypeReplica)
	perfSessions := make(map[string]*aggregate.PerfSession)
	for _, n := range nodes {
		if n.Session() == nil {
			return fmt.Errorf("init node failed = %s", n.TCPAddr())
		}
		perf := client.Nodes.GetPerfSession(n.TCPAddr(), session.NodeTypeReplica)
		if perf == nil {
			return fmt.Errorf("get perf-node failed, node=%s", n.TCPAddr())
		}
		if perf.NodeSession == nil {
			return fmt.Errorf("session err, node=%s", n.TCPAddr())
		}
		perfSessions[n.CombinedAddr()] = perf
	}
	err = checkUnConfirmedDecree(perfSessions)
	if err != nil {
		return err
	}
	//4. set env config deny write request
	toolkits.LogInfo("set env config deny write request")
	var envs = map[string]string{
		"replica.deny_client_request": "timeout*write",
	}
	err = client.Meta.UpdateAppEnvs(table, envs)
	if err != nil {
		return err
	}
	//5. check duplicate qps if equal 0
	toolkits.LogInfo("check duplicate qps if equal 0")
	resp, err := client.Meta.QueryConfig(table)
	if err != nil {
		return err
	}
	err = checkDuplicatingQPS(perfSessions, resp.AppID)
	if err != nil {
		return err
	}
	//6. switch table addrs in metaproxy
	toolkits.LogInfo("switch table addrs in metaproxy")
	if metaProxyZkRoot == "" {
		toolkits.LogWarn("can't switch cluster via metaproxy for you don't specify enough meta proxy info, please manual-switch the table cluster!")
		return nil
	}
	err = SwitchMetaAddrs(client, metaProxyZkAddrs, metaProxyZkRoot, table, targetAddrs)
	if err != nil {
		return err
	}
	return nil
}

func checkUnConfirmedDecree(perfSessions map[string]*aggregate.PerfSession) error {
	completed := false
	for !completed {
		completed = true
		time.Sleep(10 * time.Second)
		for addr, perf := range perfSessions {
			stats, err := perf.GetPerfCounters("pending_mutations_count")
			if err != nil {
				return err
			}
			if len(stats) != 1 {
				return fmt.Errorf("get pending_mutations_count perfcounter size must be 1, but now is %d", len(stats))
			}

			if stats[0].Value > pendingMutationThreshold {
				completed = false
				toolkits.LogInfo(fmt.Sprintf("%s has pending_mutations_count %f", addr, stats[0].Value))
				break
			}
		}
	}
	toolkits.LogInfo(fmt.Sprintf("all the node pending_mutations_count has less %f", pendingMutationThreshold))
	time.Sleep(10 * time.Second)
	return nil
}

func checkDuplicatingQPS(perfSessions map[string]*aggregate.PerfSession, tableID int32) error {
	completed := false
	counter := fmt.Sprintf("dup_shipped_ops@%d", tableID)
	for !completed {
		completed = true
		time.Sleep(10 * time.Second)
		for addr, perf := range perfSessions {
			stats := util.GetPartitionStat(perf, counter)
			for gpid, qps := range stats {
				if qps > 0 {
					completed = false
					toolkits.LogInfo(fmt.Sprintf("%s[%s] still sending pending mutation %f", addr, gpid, qps))
					break
				}
			}
		}
	}
	toolkits.LogInfo("all the node has stop duplicate the pending wal")
	time.Sleep(10 * time.Second)
	return nil
}
