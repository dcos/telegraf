# DC/OS Metrics Output Plugin

The DC/OS Metrics output provides a [DC/OS Metrics API](https://docs.mesosphere.com/1.11/metrics/metrics-api/) server that exposes metrics collected by Telegraf.

This plugin is not supported on Windows. If enabled on Windows, it will do nothing.

### Configuration:

```toml
# Configuration for the DC/OS Metrics API output plugin.
[[outputs.dcos_metrics]]
  # Address to listen on. Leave unset to listen on a systemd-provided socket.
  listen = ":8080"

  # Systemd socket name to listen on. Leave unset to listen on a port.
  #systemd_socket_name = "dcos-metrics.socket"

  # Duration to cache metrics in memory.
  cache_expiry = "2m"

  # DC/OS node's role (master or agent).
  dcos_node_role = "agent"

  # DC/OS node's private IP, as reported by /opt/mesosphere/bin/detect_ip.
  dcos_node_private_ip = "10.10.0.1"

  # Local Mesos instance's ID.
  mesos_id = "ABCDEF1234"

  # Global DC/OS Cluster ID.
  dcos_cluster_id = "4321FEDCBA"
```
