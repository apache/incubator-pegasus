package tablemigrator

import (
	"fmt"
	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits/metaproxy"
	"github.com/apache/incubator-pegasus/admin-cli/util"
	"github.com/apache/incubator-pegasus/go-client/session"
	"github.com/pegasus-kv/collector/aggregate"
	"time"
)

/**
1. check data version
2. create table duplication
3. check confirm decree if < 5k
4. set env config deny write request
5. check duplicate qps decree if == 0
6. switch table env addrs
*/

func MigrateTable(client *executor.Client, table string, toCluster string) error {
	//1. check data version
	version, err := executor.QueryReplicaDataVersion(client, table)
	if err != nil {
		return nil
	}
	if version.DataVersion != "1" {
		return fmt.Errorf("not support data version = 0 to migrate by duplication")
	}

	//2. create data version
	err = executor.AddDuplication(client, table, toCluster, false)
	if err != nil {
		return err
	}

	//3. check un-confirm decree is less 5k
	nodes := client.Nodes.GetAllNodes(session.NodeTypeReplica)
	var perfSessions []*aggregate.PerfSession
	for _, n := range nodes {
		perfSessions = append(perfSessions, client.Nodes.GetPerfSession(n.TCPAddr(), session.NodeTypeReplica))
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
	err = checkDuplicateQPS(perfSessions, resp.AppID)
	if err != nil {
		return err
	}
	//6. switch table addrs in metaproxy
	err = metaproxy.SwitchMetaAddrs(client, "", "", "", "")
	if err != nil {
		return err
	}
	return nil
}

func checkUnConfirmedDecree(perfSessions []*aggregate.PerfSession, threshold float64) error {
	completed := false
	for !completed {
		completed = true
		time.Sleep(10 * time.Second)
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
	return nil
}

func checkDuplicateQPS(perfSessions []*aggregate.PerfSession, tableID int32) error {
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
	return nil
}
