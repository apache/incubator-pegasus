# Pegasus with Docker

## HOW TO

To start one cluster, internally we use docker-compose to set up dockers:

```sh
./start_onebox.sh
```

To start multiple clusters, you should reconfigure the cluster_args.sh by
using a different `CLUSTER_NAME` and `NODE_IP_PREFIX`. For example `onebox2` and `172.22.0`.
