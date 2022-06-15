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

func MigrateTable(client *executor.Client, table string, metaProxyZkAddrs string, metaProxyZkRoot string, targetCluster string, targetAddrs string) error {
	//1. check data version
	version, err := executor.QueryReplicaDataVersion(client, table)
	if err != nil {
		return err
	}
	if version.DataVersion != "1" {
		return fmt.Errorf("not support migrate table with data_version = %s by duplication", version.DataVersion)
	}

	//2. create table duplication
	/** err = executor.AddDuplication(client, table, targetCluster, true)
	if err != nil {
		return err
	}**/

	//3. check un-confirm decree if less 5k
	nodes := client.Nodes.GetAllNodes(session.NodeTypeReplica)
	var perfSessions []*aggregate.PerfSession
	for _, n := range nodes {
		perf := client.Nodes.GetPerfSession(n.TCPAddr(), session.NodeTypeReplica)
		if perf == nil {
			return fmt.Errorf("get perf-node failed, node=%s", n.TCPAddr())
		}
		if perf.NodeSession == nil {
			return fmt.Errorf("session err, node=%s", n.TCPAddr())
		}
		perfSessions = append(perfSessions, perf)
	}
	err = checkUnConfirmedDecree(perfSessions, 5000)
	if err != nil {
		return err
	}
	//4. set env config deny write request
	var envs = map[string]string{
		"replica.deny_client_request": "timeout*write",
	}
	err = client.Meta.UpdateAppEnvs(table, envs)
	if err != nil {
		return err
	}
	//5. check duplicate qps if equal 0
	resp, err := client.Meta.QueryConfig(table)
	if err != nil {
		return err
	}
	err = checkDuplicatingQPS(perfSessions, resp.AppID)
	if err != nil {
		return err
	}
	//6. switch table addrs in metaproxy
	if metaProxyZkRoot == "" {
		toolkits.LogWarn("you don't specify enough meta proxy info, please manual-switch the table cluster!")
		return nil
	}
	err = SwitchMetaAddrs(client, metaProxyZkAddrs, metaProxyZkRoot, table, targetAddrs)
	if err != nil {
		return err
	}
	return nil
}

func checkUnConfirmedDecree(perfSessions []*aggregate.PerfSession, threshold float64) error {
	completed := false
	for !completed {
		completed = true
		time.Sleep(1 * time.Second)
		for _, perf := range perfSessions {
			stats, err := perf.GetPerfCounters("pending_mutations_count")
			if err != nil {
				return err
			}
			if len(stats) != 1 {
				return fmt.Errorf("get pending_mutations_count perfcounter size must be 1, but now is %d", len(stats))
			}

			if stats[0].Value > threshold {
				completed = false
				toolkits.LogDebug(fmt.Sprintf("%s has pending_mutations_count %f", perf.Address, stats[0].Value))
				break
			}
		}
	}
	toolkits.LogDebug(fmt.Sprintf("all the node pending_mutations_count has less %f", threshold))
	return nil
}

func checkDuplicatingQPS(perfSessions []*aggregate.PerfSession, tableID int32) error {
	completed := false
	counter := fmt.Sprintf("duplicate_qps@%d", tableID)
	for !completed {
		completed = true
		time.Sleep(10 * time.Second)
		for _, perf := range perfSessions {
			stats := util.GetPartitionStat(perf, counter)
			for gpid, qps := range stats {
				if qps > 0 {
					completed = false
					toolkits.LogDebug(fmt.Sprintf("%s[%s] still sending pending mutation %f", perf.Address, gpid, qps))
					break
				}
			}
		}
	}
	toolkits.LogDebug("all the node has stop duplicate the pending wal")
	return nil
}
