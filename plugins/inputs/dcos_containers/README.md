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
  mesos_agent_url = "http://$NODE_PRIVATE_IP:5051"
  ## The period after which requests to mesos agent should time out
  timeout = "10s"
  ## The user agent to send with requests
  user_agent = "Telegraf-dcos-containers"
  ## Optional IAM configuration
  # ca_certificate_path = "/run/dcos/pki/CA/ca-bundle.crt"
  # iam_config_path = "/run/dcos/etc/dcos-telegraf/service_account.json"
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
   - tags:
     - volume_persistence_id
     - volume_persistence_principal
   - fields:
     - limit_bytes
     - used_bytes

 - net
   - prefixes:
     - rx_
     - tx_
     - ip_
     - icmp_
     - tcp_
     - udp_
   - fields:
     - packets
     - bytes
     - errors
     - dropped
     - rtt_microsecs_p50
     - rtt_microsecs_p90
     - rtt_microsecs_p95
     - rtt_microsecs_p99
     - active_connections
     - time_wait_connections
     - forwarding
     - default_ttl
     - in_receives
     - in_hdr_errors
     - in_addr_errors
     - forw_datagrams
     - in_unknown_protos
     - in_discards
     - in_delivers
     - out_requests
     - out_discards
     - out_no_routes
     - reasm_timeout
     - reasm_reqds
     - reasm_oks
     - reasm_fails
     - frag_oks
     - frag_fails
     - frag_creates
     - in_msgs
     - in_errors
     - in_csum_errors
     - in_dest_unreachs
     - in_time_excds
     - in_parm_probs
     - in_src_quenchs
     - in_redirects
     - in_echos
     - in_echo_reps
     - in_timestamps
     - in_timestamp_reps
     - in_addr_masks
     - in_addr_mark_reps
     - out_msgs
     - out_errors
     - out_dest_unreachs
     - out_time_excds
     - out_parm_probs
     - out_src_quenchs
     - out_redirects
     - out_echos
     - out_echo_reps
     - out_timestamps
     - out_timestamp_reps
     - out_addr_masks
     - out_addr_mask_reps
     - rto_algorithm
     - rto_min
     - rto_max
     - max_conn
     - active_opens
     - passive_opens
     - attempt_fails
     - estab_resets
     - curr_estab
     - in_segs
     - out_segs
     - retrans_segs
     - in_errs
     - out_rsts
     - in_datagrams
     - no_ports
     - out_datagrams
     - rcvbuf_errors
     - sndbuf_errors
     - in_csum_errors
     - ignored_multi

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

 - perf
   - fields:
     - timestamp
     - duration
     - cycles
     - stalled_cycles_frontend
     - stalled_cycles_backend
     - instructions
     - cache_references
     - cache_misses
     - branches
     - branch_misses
     - bus_cycles
     - ref_cycles
     - cpu_clock
     - task_clock
     - page_faults
     - minor_faults
     - major_faults
     - context_switches
     - cpu_migrations
     - alignment_faults
     - emulation_faults
     - l1_dcache_loads
     - l1_dcache_load_misses
     - l1_dcache_stores
     - l1_dcache_store_misses
     - l1_dcache_prefetches
     - l1_dcache_prefetch_misses
     - l1_icache_loads
     - l1_icache_load_misses
     - l1_icache_prefetches
     - l1_icache_prefetch_misses
     - llc_loads
     - llc_load_misses
     - llc_stores
     - llc_store_misses
     - llc_prefetches
     - llc_prefetch_misses
     - dtlb_loads
     - dtlb_load_misses
     - dtlb_stores
     - dtlb_store_misses
     - dtlb_prefetches
     - dtlb_prefetch_misses
     - itlb_loads
     - itlb_load_misses
     - branch_loads
     - branch_load_misses
     - node_loads
     - node_load_misses
     - node_stores
     - node_store_misses
     - node_prefetches
     - node_prefetch_misses
 
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
