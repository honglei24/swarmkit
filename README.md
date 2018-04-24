# [SwarmKit](https://github.com/docker/swarmkit)
在社区版的基础上增加了修改raft日志的功能，可以用于灾难恢复时的数据修复。

### Node Management

```
$ ./swarm-rafttool append --help
update raft log

Usage:
  ./swarm-rafttool append [flags]

Flags:
      --image string          Service id.
      --network-id string     Service network.
      --node-id string        Node id.
      --replicas uint         Service replicas.
      --service-id string     Service id.
      --service-name string   Service name.
      --slot uint             Task slot.
      --task-id string        Task id.
      --type string           Object type node/service/task/network...

Global Flags:
  -d, --state-dir string    State directory (default "/var/lib/swarmd")
      --unlock-key string   Unlock key, if raft logs are encrypted
```

