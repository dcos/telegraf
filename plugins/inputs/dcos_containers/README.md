# DC/OS Containers Plugin

The DC/OS containers plugin gathers metrics about the resource consumption of
containers being run by the local Mesos agent. 

### Configuration:

This section contains the default TOML to configure the plugin.  You can
generate it using `telegraf --usage dcos_containers`.

```toml
# Telegraf plugin for gathering resource metrics about mesos containers
[[inputs.dcos_containers]]
  ## The URL of the mesos agent
  mesos_agent_url = "http://localhost:5051"
  ## The period after which requests to mesos agent should time out
  timeout = 10s
```

### Metrics:

 - container
   - fields:
     - processes
     - threads

 - cpus
   - fields:
     - user_time_secs
     - system_time_secs
     - limit
     - nr_periods
     - nr_throttled
     - throttled_time_secs

 - mem
   - fields:
     - total_bytes
     - total_memsw_bytes
     - limit_bytes
     - soft_limit_bytes
     - cache_bytes
     - rss_bytes
     - mapped_file_bytes
     - swap_bytes
     - unevictable_bytes
     - low_pressure_counter
     - medium_pressure_counter
     - critical_pressure_counter

 - disk
   - fields:
     - limit_bytes
     - used_bytes

 - net
   - tags:
     - rx_tx
   - fields:
     - packets
     - bytes
     - errors
     - dropped

 - blkio
   - tags:
     - policy <!-- cfq/cfq_recursive/throttling -->
     - device <!-- eg 1.4 -->
   - fields:
     - io_serviced
     - io_service_bytes
     - io_service_time
     - io_merged
     - io_queued
     - io_wait_time
 
### Tags:

All metrics have the following tag:

 - container_id

### Example Output:

<!-- TODO: expand with all metrics -->
```
$ telegraf --config dcos.conf --input-filter dcos_container --test
* Plugin: dcos_container
    cpus,host=172.17.8.102,container_id=12377985-615c-4a1a-a491-721ce7cd807a user_time_secs=10,system_time_secs=1,limit=4,nr_periods=11045,nr_throttled=132,throttled_time_seconds=1 1453831884664956455
```
