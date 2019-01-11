package dcos_containers

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/dcosutil"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/agent"
	"github.com/mesos/mesos-go/api/v1/lib/agent/calls"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpagent"
)

const sampleConfig = `
  ## The URL of the local mesos agent
  mesos_agent_url = "http://$NODE_PRIVATE_IP:5051"
  ## The period after which requests to mesos agent should time out
  # timeout = "10s"
  ## The user agent to send with requests
  user_agent = "telegraf-dcos-containers"
  ## Optional IAM configuration
  # ca_certificate_path = "/run/dcos/pki/CA/ca-bundle.crt"
  # iam_config_path = "/run/dcos/etc/dcos-telegraf/service_account.json"
`

// DCOSContainers describes the options available to this plugin
type DCOSContainers struct {
	MesosAgentUrl string
	Timeout       internal.Duration
	client        *httpcli.Client
	dcosutil.DCOSConfig
}

// measurement is a combination of fields and tags specific to those fields
type measurement struct {
	name   string
	fields map[string]interface{}
	tags   map[string]string
}

// combineTags combines this measurement's tags with some other tags. In the
// event of a collision, this measurement's tags take priority.
func (m *measurement) combineTags(newTags map[string]string) map[string]string {
	results := make(map[string]string)
	for k, v := range newTags {
		results[k] = v
	}
	for k, v := range m.tags {
		results[k] = v
	}
	return results
}

// newMeasurement is a convenience method for instantiating new measurements
func newMeasurement(name string) measurement {
	return measurement{
		name:   name,
		fields: make(map[string]interface{}),
		tags:   make(map[string]string),
	}
}

// SampleConfig returns the default configuration
func (dc *DCOSContainers) SampleConfig() string {
	return sampleConfig
}

// Description returns a one-sentence description of dcos_containers
func (dc *DCOSContainers) Description() string {
	return "Plugin for monitoring mesos container resource consumption"
}

// Gather takes in an accumulator and adds the metrics that the plugin gathers.
// It is invoked on a schedule (default every 10s) by the telegraf runtime.
func (dc *DCOSContainers) Gather(acc telegraf.Accumulator) error {
	client, err := dc.getClient()
	if err != nil {
		return err
	}

	cli := httpagent.NewSender(client.Send)
	ctx, cancel := context.WithTimeout(context.Background(), dc.Timeout.Duration)
	defer cancel()

	gc, err := dc.getContainers(ctx, cli)
	if err != nil {
		return err
	}

	for _, c := range gc.Containers {
		ts, tsOK := cTS(c)
		tags := cTags(c)
		for _, m := range cMeasurements(c) {
			if len(m.fields) > 0 {
				if tsOK {
					acc.AddFields(m.name, m.fields, m.combineTags(tags), ts)
				} else {
					acc.AddFields(m.name, m.fields, m.combineTags(tags))
				}
			}
		}
	}

	return nil
}

// getContainers requests a list of containers from the operator API
func (dc *DCOSContainers) getContainers(ctx context.Context, cli calls.Sender) (*agent.Response_GetContainers, error) {
	resp, err := cli.Send(ctx, calls.NonStreaming(calls.GetContainers()))
	if err != nil {
		return nil, err
	}
	r, err := processResponse(resp, agent.Response_GET_CONTAINERS)
	if err != nil {
		return nil, err
	}

	gc := r.GetGetContainers()
	if gc == nil {
		return &agent.Response_GetContainers{Containers: []agent.Response_GetContainers_Container{}}, nil
	}

	return gc, nil
}

// getClient returns an httpcli client configured with the available levels of
// TLS and IAM according to flags set in the config
func (dc *DCOSContainers) getClient() (*httpcli.Client, error) {
	if dc.client != nil {
		return dc.client, nil
	}

	uri := dc.MesosAgentUrl + "/api/v1"
	client := httpcli.New(httpcli.Endpoint(uri), httpcli.DefaultHeader("User-Agent",
		dcosutil.GetUserAgent(dc.UserAgent)))
	cfgOpts := []httpcli.ConfigOpt{}
	opts := []httpcli.Opt{}

	var rt http.RoundTripper
	var err error

	if dc.CACertificatePath != "" {
		if rt, err = dc.DCOSConfig.Transport(); err != nil {
			return nil, fmt.Errorf("error creating transport: %s", err)
		}
		if dc.IAMConfigPath != "" {
			cfgOpts = append(cfgOpts, httpcli.RoundTripper(rt))
		}
	}
	opts = append(opts, httpcli.Do(httpcli.With(cfgOpts...)))
	client.With(opts...)

	dc.client = client
	return client, nil
}

// processResponse reads the response from a triggered request, verifies its
// type, and returns an agent response
func processResponse(resp mesos.Response, t agent.Response_Type) (agent.Response, error) {
	var r agent.Response
	defer func() {
		if resp != nil {
			resp.Close()
		}
	}()
	for {
		if err := resp.Decode(&r); err != nil {
			if err == io.EOF {
				break
			}
			return r, err
		}
	}
	if r.GetType() == t {
		return r, nil
	} else {
		return r, fmt.Errorf("processResponse expected type %q, got %q", t, r.GetType())
	}
}

// cMeasurements flattens a Container object into a slice of measurements with
// fields and tags
func cMeasurements(c agent.Response_GetContainers_Container) []measurement {
	container := newMeasurement("container")
	cpus := newMeasurement("cpus")
	mem := newMeasurement("mem")
	disk := newMeasurement("disk")
	net := newMeasurement("net")

	rs := c.GetResourceStatistics()
	if rs == nil {
		return []measurement{}
	}

	results := []measurement{
		container, cpus, mem, disk, net,
	}

	// These items are not in alphabetical order; instead we preserve the order
	// in the source of the ResourceStatistics struct to make it easy to update.
	warnIfNotSet(setIfNotNil(container.fields, "processes", rs.GetProcesses))
	warnIfNotSet(setIfNotNil(container.fields, "threads", rs.GetThreads))

	warnIfNotSet(setIfNotNil(cpus.fields, "user_time_secs", rs.GetCPUsUserTimeSecs))
	warnIfNotSet(setIfNotNil(cpus.fields, "system_time_secs", rs.GetCPUsSystemTimeSecs))
	warnIfNotSet(setIfNotNil(cpus.fields, "limit", rs.GetCPUsLimit))
	warnIfNotSet(setIfNotNil(cpus.fields, "nr_periods", rs.GetCPUsNrPeriods))
	warnIfNotSet(setIfNotNil(cpus.fields, "nr_throttled", rs.GetCPUsNrThrottled))
	warnIfNotSet(setIfNotNil(cpus.fields, "throttled_time_secs", rs.GetCPUsThrottledTimeSecs))

	warnIfNotSet(setIfNotNil(mem.fields, "total_bytes", rs.GetMemTotalBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "total_memsw_bytes", rs.GetMemTotalMemswBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "limit_bytes", rs.GetMemLimitBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "soft_limit_bytes", rs.GetMemSoftLimitBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "file_bytes", rs.GetMemFileBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "anon_bytes", rs.GetMemAnonBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "cache_bytes", rs.GetMemCacheBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "rss_bytes", rs.GetMemRSSBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "mapped_file_bytes", rs.GetMemMappedFileBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "swap_bytes", rs.GetMemSwapBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "unevictable_bytes", rs.GetMemUnevictableBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "low_pressure_counter", rs.GetMemLowPressureCounter))
	warnIfNotSet(setIfNotNil(mem.fields, "medium_pressure_counter", rs.GetMemMediumPressureCounter))
	warnIfNotSet(setIfNotNil(mem.fields, "critical_pressure_counter", rs.GetMemCriticalPressureCounter))

	warnIfNotSet(setIfNotNil(disk.fields, "limit_bytes", rs.GetDiskLimitBytes))
	warnIfNotSet(setIfNotNil(disk.fields, "used_bytes", rs.GetDiskUsedBytes))

	if ds := rs.GetDiskStatistics(); ds != nil {
		results = append(results, cDiskStatistics(ds)...)
	}

	if bs := rs.GetBlkioStatistics(); bs != nil {
		results = append(results, cBlkioMeasurements(*bs)...)
	}

	if perf := rs.GetPerf(); perf != nil {
		m := newMeasurement("perf")
		warnIfNotSet(setIfNotNil(m.fields, "timestamp", perf.GetTimestamp))
		warnIfNotSet(setIfNotNil(m.fields, "duration", perf.GetDuration))
		warnIfNotSet(setIfNotNil(m.fields, "cycles", perf.GetCycles))
		warnIfNotSet(setIfNotNil(m.fields, "stalled_cycles_frontend", perf.GetStalledCyclesFrontend))
		warnIfNotSet(setIfNotNil(m.fields, "stalled_cycles_backend", perf.GetStalledCyclesBackend))
		warnIfNotSet(setIfNotNil(m.fields, "instructions", perf.GetInstructions))
		warnIfNotSet(setIfNotNil(m.fields, "cache_references", perf.GetCacheReferences))
		warnIfNotSet(setIfNotNil(m.fields, "cache_misses", perf.GetCacheMisses))
		warnIfNotSet(setIfNotNil(m.fields, "branches", perf.GetBranches))
		warnIfNotSet(setIfNotNil(m.fields, "branch_misses", perf.GetBranchMisses))
		warnIfNotSet(setIfNotNil(m.fields, "bus_cycles", perf.GetBusCycles))
		warnIfNotSet(setIfNotNil(m.fields, "ref_cycles", perf.GetRefCycles))
		warnIfNotSet(setIfNotNil(m.fields, "cpu_clock", perf.GetCPUClock))
		warnIfNotSet(setIfNotNil(m.fields, "task_clock", perf.GetTaskClock))
		warnIfNotSet(setIfNotNil(m.fields, "page_faults", perf.GetPageFaults))
		warnIfNotSet(setIfNotNil(m.fields, "minor_faults", perf.GetMinorFaults))
		warnIfNotSet(setIfNotNil(m.fields, "major_faults", perf.GetMajorFaults))
		warnIfNotSet(setIfNotNil(m.fields, "context_switches", perf.GetContextSwitches))
		warnIfNotSet(setIfNotNil(m.fields, "cpu_migrations", perf.GetCPUMigrations))
		warnIfNotSet(setIfNotNil(m.fields, "alignment_faults", perf.GetAlignmentFaults))
		warnIfNotSet(setIfNotNil(m.fields, "emulation_faults", perf.GetEmulationFaults))
		warnIfNotSet(setIfNotNil(m.fields, "l1_dcache_loads", perf.GetL1DcacheLoads))
		warnIfNotSet(setIfNotNil(m.fields, "l1_dcache_load_misses", perf.GetL1DcacheLoadMisses))
		warnIfNotSet(setIfNotNil(m.fields, "l1_dcache_stores", perf.GetL1DcacheStores))
		warnIfNotSet(setIfNotNil(m.fields, "l1_dcache_store_misses", perf.GetL1DcacheStoreMisses))
		warnIfNotSet(setIfNotNil(m.fields, "l1_dcache_prefetches", perf.GetL1DcachePrefetches))
		warnIfNotSet(setIfNotNil(m.fields, "l1_dcache_prefetch_misses", perf.GetL1DcachePrefetchMisses))
		warnIfNotSet(setIfNotNil(m.fields, "l1_icache_loads", perf.GetL1IcacheLoads))
		warnIfNotSet(setIfNotNil(m.fields, "l1_icache_load_misses", perf.GetL1IcacheLoadMisses))
		warnIfNotSet(setIfNotNil(m.fields, "l1_icache_prefetches", perf.GetL1IcachePrefetches))
		warnIfNotSet(setIfNotNil(m.fields, "l1_icache_prefetch_misses", perf.GetL1IcachePrefetchMisses))
		warnIfNotSet(setIfNotNil(m.fields, "llc_loads", perf.GetLLCLoads))
		warnIfNotSet(setIfNotNil(m.fields, "llc_load_misses", perf.GetLLCLoadMisses))
		warnIfNotSet(setIfNotNil(m.fields, "llc_stores", perf.GetLLCStores))
		warnIfNotSet(setIfNotNil(m.fields, "llc_store_misses", perf.GetLLCStoreMisses))
		warnIfNotSet(setIfNotNil(m.fields, "llc_prefetches", perf.GetLLCPrefetches))
		warnIfNotSet(setIfNotNil(m.fields, "llc_prefetch_misses", perf.GetLLCPrefetchMisses))
		warnIfNotSet(setIfNotNil(m.fields, "dtlb_loads", perf.GetDTLBLoads))
		warnIfNotSet(setIfNotNil(m.fields, "dtlb_load_misses", perf.GetDTLBLoadMisses))
		warnIfNotSet(setIfNotNil(m.fields, "dtlb_stores", perf.GetDTLBStores))
		warnIfNotSet(setIfNotNil(m.fields, "dtlb_store_misses", perf.GetDTLBStoreMisses))
		warnIfNotSet(setIfNotNil(m.fields, "dtlb_prefetches", perf.GetDTLBPrefetches))
		warnIfNotSet(setIfNotNil(m.fields, "dtlb_prefetch_misses", perf.GetDTLBPrefetchMisses))
		warnIfNotSet(setIfNotNil(m.fields, "itlb_loads", perf.GetITLBLoads))
		warnIfNotSet(setIfNotNil(m.fields, "itlb_load_misses", perf.GetITLBLoadMisses))
		warnIfNotSet(setIfNotNil(m.fields, "branch_loads", perf.GetBranchLoads))
		warnIfNotSet(setIfNotNil(m.fields, "branch_load_misses", perf.GetBranchLoadMisses))
		warnIfNotSet(setIfNotNil(m.fields, "node_loads", perf.GetNodeLoads))
		warnIfNotSet(setIfNotNil(m.fields, "node_load_misses", perf.GetNodeLoadMisses))
		warnIfNotSet(setIfNotNil(m.fields, "node_stores", perf.GetNodeStores))
		warnIfNotSet(setIfNotNil(m.fields, "node_store_misses", perf.GetNodeStoreMisses))
		warnIfNotSet(setIfNotNil(m.fields, "node_prefetches", perf.GetNodePrefetches))
		warnIfNotSet(setIfNotNil(m.fields, "node_prefetch_misses", perf.GetNodePrefetchMisses))

		results = append(results, m)
	}

	warnIfNotSet(setIfNotNil(net.fields, "rx_packets", rs.GetNetRxPackets))
	warnIfNotSet(setIfNotNil(net.fields, "rx_bytes", rs.GetNetRxBytes))
	warnIfNotSet(setIfNotNil(net.fields, "rx_errors", rs.GetNetRxErrors))
	warnIfNotSet(setIfNotNil(net.fields, "rx_dropped", rs.GetNetRxDropped))
	warnIfNotSet(setIfNotNil(net.fields, "tx_packets", rs.GetNetTxPackets))
	warnIfNotSet(setIfNotNil(net.fields, "tx_bytes", rs.GetNetTxBytes))
	warnIfNotSet(setIfNotNil(net.fields, "tx_errors", rs.GetNetTxErrors))
	warnIfNotSet(setIfNotNil(net.fields, "tx_dropped", rs.GetNetTxDropped))
	warnIfNotSet(setIfNotNil(net.fields, "tcp_rtt_microsecs_p50", rs.GetNetTCPRttMicrosecsP50))
	warnIfNotSet(setIfNotNil(net.fields, "tcp_rtt_microsecs_p90", rs.GetNetTCPRttMicrosecsP90))
	warnIfNotSet(setIfNotNil(net.fields, "tcp_rtt_microsecs_p95", rs.GetNetTCPRttMicrosecsP95))
	warnIfNotSet(setIfNotNil(net.fields, "tcp_rtt_microsecs_p99", rs.GetNetTCPRttMicrosecsP99))
	warnIfNotSet(setIfNotNil(net.fields, "tcp_active_connections", rs.GetNetTCPActiveConnections))
	warnIfNotSet(setIfNotNil(net.fields, "tcp_time_wait_connections", rs.GetNetTCPTimeWaitConnections))

	if ntcs := rs.GetNetTrafficControlStatistics(); ntcs != nil {
		results = append(results, cNetTrafficControlStatistics(ntcs)...)
	}

	if snmp := rs.GetNetSNMPStatistics(); snmp != nil {
		if ipStats := snmp.GetIPStats(); ipStats != nil {
			warnIfNotSet(setIfNotNil(net.fields, "ip_forwarding", ipStats.GetForwarding))
			warnIfNotSet(setIfNotNil(net.fields, "ip_default_ttl", ipStats.GetDefaultTTL))
			warnIfNotSet(setIfNotNil(net.fields, "ip_in_receives", ipStats.GetInReceives))
			warnIfNotSet(setIfNotNil(net.fields, "ip_in_hdr_errors", ipStats.GetInHdrErrors))
			warnIfNotSet(setIfNotNil(net.fields, "ip_in_addr_errors", ipStats.GetInAddrErrors))
			warnIfNotSet(setIfNotNil(net.fields, "ip_forw_datagrams", ipStats.GetForwDatagrams))
			warnIfNotSet(setIfNotNil(net.fields, "ip_in_unknown_protos", ipStats.GetInUnknownProtos))
			warnIfNotSet(setIfNotNil(net.fields, "ip_in_discards", ipStats.GetInDiscards))
			warnIfNotSet(setIfNotNil(net.fields, "ip_in_delivers", ipStats.GetInDelivers))
			warnIfNotSet(setIfNotNil(net.fields, "ip_out_requests", ipStats.GetOutRequests))
			warnIfNotSet(setIfNotNil(net.fields, "ip_out_discards", ipStats.GetOutDiscards))
			warnIfNotSet(setIfNotNil(net.fields, "ip_out_no_routes", ipStats.GetOutNoRoutes))
			warnIfNotSet(setIfNotNil(net.fields, "ip_reasm_timeout", ipStats.GetReasmTimeout))
			warnIfNotSet(setIfNotNil(net.fields, "ip_reasm_reqds", ipStats.GetReasmReqds))
			warnIfNotSet(setIfNotNil(net.fields, "ip_reasm_oks", ipStats.GetReasmOKs))
			warnIfNotSet(setIfNotNil(net.fields, "ip_reasm_fails", ipStats.GetReasmFails))
			warnIfNotSet(setIfNotNil(net.fields, "ip_frag_oks", ipStats.GetFragOKs))
			warnIfNotSet(setIfNotNil(net.fields, "ip_frag_fails", ipStats.GetFragFails))
			warnIfNotSet(setIfNotNil(net.fields, "ip_frag_creates", ipStats.GetFragCreates))
		}

		if icmpStats := snmp.GetICMPStats(); icmpStats != nil {
			warnIfNotSet(setIfNotNil(net.fields, "icmp_in_msgs", icmpStats.GetInMsgs))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_in_errors", icmpStats.GetInErrors))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_in_csum_errors", icmpStats.GetInCsumErrors))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_in_dest_unreachs", icmpStats.GetInDestUnreachs))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_in_time_excds", icmpStats.GetInTimeExcds))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_in_parm_probs", icmpStats.GetInParmProbs))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_in_src_quenchs", icmpStats.GetInSrcQuenchs))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_in_redirects", icmpStats.GetInRedirects))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_in_echos", icmpStats.GetInEchos))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_in_echo_reps", icmpStats.GetInEchoReps))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_in_timestamps", icmpStats.GetInTimestamps))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_in_timestamp_reps", icmpStats.GetInTimestampReps))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_in_addr_masks", icmpStats.GetInAddrMasks))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_in_addr_mark_reps", icmpStats.GetInAddrMaskReps))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_out_msgs", icmpStats.GetOutMsgs))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_out_errors", icmpStats.GetOutErrors))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_out_dest_unreachs", icmpStats.GetOutDestUnreachs))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_out_time_excds", icmpStats.GetOutTimeExcds))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_out_parm_probs", icmpStats.GetOutParmProbs))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_out_src_quenchs", icmpStats.GetOutSrcQuenchs))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_out_redirects", icmpStats.GetOutRedirects))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_out_echos", icmpStats.GetOutEchos))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_out_echo_reps", icmpStats.GetOutEchoReps))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_out_timestamps", icmpStats.GetOutTimestamps))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_out_timestamp_reps", icmpStats.GetOutTimestampReps))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_out_addr_masks", icmpStats.GetOutAddrMasks))
			warnIfNotSet(setIfNotNil(net.fields, "icmp_out_addr_mask_reps", icmpStats.GetOutAddrMaskReps))
		}

		if tcpStats := snmp.GetTCPStats(); tcpStats != nil {
			warnIfNotSet(setIfNotNil(net.fields, "tcp_rto_algorithm", tcpStats.GetRtoAlgorithm))
			warnIfNotSet(setIfNotNil(net.fields, "tcp_rto_min", tcpStats.GetRtoMin))
			warnIfNotSet(setIfNotNil(net.fields, "tcp_rto_max", tcpStats.GetRtoMax))
			warnIfNotSet(setIfNotNil(net.fields, "tcp_max_conn", tcpStats.GetMaxConn))
			warnIfNotSet(setIfNotNil(net.fields, "tcp_active_opens", tcpStats.GetActiveOpens))
			warnIfNotSet(setIfNotNil(net.fields, "tcp_passive_opens", tcpStats.GetPassiveOpens))
			warnIfNotSet(setIfNotNil(net.fields, "tcp_attempt_fails", tcpStats.GetAttemptFails))
			warnIfNotSet(setIfNotNil(net.fields, "tcp_estab_resets", tcpStats.GetEstabResets))
			warnIfNotSet(setIfNotNil(net.fields, "tcp_curr_estab", tcpStats.GetCurrEstab))
			warnIfNotSet(setIfNotNil(net.fields, "tcp_in_segs", tcpStats.GetInSegs))
			warnIfNotSet(setIfNotNil(net.fields, "tcp_out_segs", tcpStats.GetOutSegs))
			warnIfNotSet(setIfNotNil(net.fields, "tcp_retrans_segs", tcpStats.GetRetransSegs))
			warnIfNotSet(setIfNotNil(net.fields, "tcp_in_errs", tcpStats.GetInErrs))
			warnIfNotSet(setIfNotNil(net.fields, "tcp_out_rsts", tcpStats.GetOutRsts))
			warnIfNotSet(setIfNotNil(net.fields, "tcp_in_csum_errors", tcpStats.GetInCsumErrors))
		}

		if udpStats := snmp.GetUDPStats(); udpStats != nil {
			warnIfNotSet(setIfNotNil(net.fields, "udp_in_datagrams", udpStats.GetInDatagrams))
			warnIfNotSet(setIfNotNil(net.fields, "udp_no_ports", udpStats.GetNoPorts))
			warnIfNotSet(setIfNotNil(net.fields, "udp_in_errors", udpStats.GetInErrors))
			warnIfNotSet(setIfNotNil(net.fields, "udp_out_datagrams", udpStats.GetOutDatagrams))
			warnIfNotSet(setIfNotNil(net.fields, "udp_rcvbuf_errors", udpStats.GetRcvbufErrors))
			warnIfNotSet(setIfNotNil(net.fields, "udp_sndbuf_errors", udpStats.GetSndbufErrors))
			warnIfNotSet(setIfNotNil(net.fields, "udp_in_csum_errors", udpStats.GetInCsumErrors))
			warnIfNotSet(setIfNotNil(net.fields, "udp_ignored_multi", udpStats.GetIgnoredMulti))
		}
	}

	return results
}

// cDiskStatistics tags each set of disk statistics with the
// volume persistence ID and principal, if given
func cDiskStatistics(ds []mesos.DiskStatistics) []measurement {
	var results []measurement

	for _, disk := range ds {
		m := newMeasurement("disk")
		if p := disk.GetPersistence(); p != nil {
			m.tags["volume_persistence_id"] = p.GetID()
			m.tags["volume_persistence_principal"] = p.GetPrincipal()
		}
		warnIfNotSet(setIfNotNil(m.fields, "limit_bytes", disk.GetLimitBytes))
		warnIfNotSet(setIfNotNil(m.fields, "used_bytes", disk.GetUsedBytes))

		results = append(results, m)
	}

	return results
}

// cBlkioMeasurement flattens the deeply nested blkio_cfq statistics struct into
// a set of measurements, tagged by device ID and blkio_cfq policy
func cBlkioMeasurements(bs mesos.CgroupInfo_Blkio_Statistics) []measurement {
	var results []measurement

	ops := []mesos.CgroupInfo_Blkio_Operation{
		mesos.CgroupInfo_Blkio_UNKNOWN,
		mesos.CgroupInfo_Blkio_TOTAL,
		mesos.CgroupInfo_Blkio_READ,
		mesos.CgroupInfo_Blkio_WRITE,
		mesos.CgroupInfo_Blkio_SYNC,
		mesos.CgroupInfo_Blkio_ASYNC,
	}

	for _, cfq := range bs.GetCFQ() {
		blkio := newMeasurement("blkio")
		blkio.tags["policy"] = "cfq"
		if dev := cfq.GetDevice(); dev != nil {
			blkio.tags["device"] = fmt.Sprintf("%d.%d", dev.GetMajorNumber(), dev.GetMinorNumber())
		} else {
			blkio.tags["device"] = "default"
		}
		for _, op := range ops {
			suffix := strings.ToLower(mesos.CgroupInfo_Blkio_Operation_name[int32(op)])
			warnIfNotSet(setIfNotNil(blkio.fields, fmt.Sprintf("io_serviced_%s", suffix), blkioGetter(cfq.GetIOServiced, op)))
			warnIfNotSet(setIfNotNil(blkio.fields, fmt.Sprintf("io_service_bytes_%s", suffix), blkioGetter(cfq.GetIOServiceBytes, op)))
			warnIfNotSet(setIfNotNil(blkio.fields, fmt.Sprintf("io_service_time_%s", suffix), blkioGetter(cfq.GetIOServiceTime, op)))
			warnIfNotSet(setIfNotNil(blkio.fields, fmt.Sprintf("io_wait_time_%s", suffix), blkioGetter(cfq.GetIOWaitTime, op)))
			warnIfNotSet(setIfNotNil(blkio.fields, fmt.Sprintf("io_merged_%s", suffix), blkioGetter(cfq.GetIOMerged, op)))
			warnIfNotSet(setIfNotNil(blkio.fields, fmt.Sprintf("io_queued_%s", suffix), blkioGetter(cfq.GetIOQueued, op)))
		}

		results = append(results, blkio)
	}

	for _, cfq := range bs.GetCFQRecursive() {
		blkio := newMeasurement("blkio")
		blkio.tags["policy"] = "cfq_recursive"
		if dev := cfq.GetDevice(); dev != nil {
			blkio.tags["device"] = fmt.Sprintf("%d.%d", dev.GetMajorNumber(), dev.GetMinorNumber())
		} else {
			blkio.tags["device"] = "default"
		}
		for _, op := range ops {
			suffix := strings.ToLower(mesos.CgroupInfo_Blkio_Operation_name[int32(op)])
			warnIfNotSet(setIfNotNil(blkio.fields, fmt.Sprintf("io_serviced_%s", suffix), blkioGetter(cfq.GetIOServiced, op)))
			warnIfNotSet(setIfNotNil(blkio.fields, fmt.Sprintf("io_service_bytes_%s", suffix), blkioGetter(cfq.GetIOServiceBytes, op)))
			warnIfNotSet(setIfNotNil(blkio.fields, fmt.Sprintf("io_service_time_%s", suffix), blkioGetter(cfq.GetIOServiceTime, op)))
			warnIfNotSet(setIfNotNil(blkio.fields, fmt.Sprintf("io_wait_time_%s", suffix), blkioGetter(cfq.GetIOWaitTime, op)))
			warnIfNotSet(setIfNotNil(blkio.fields, fmt.Sprintf("io_merged_%s", suffix), blkioGetter(cfq.GetIOMerged, op)))
			warnIfNotSet(setIfNotNil(blkio.fields, fmt.Sprintf("io_queued_%s", suffix), blkioGetter(cfq.GetIOQueued, op)))
		}

		results = append(results, blkio)
	}

	for _, throttling := range bs.GetThrottling() {
		blkio := newMeasurement("blkio")
		blkio.tags["policy"] = "throttling"
		if dev := throttling.GetDevice(); dev != nil {
			blkio.tags["device"] = fmt.Sprintf("%d.%d", dev.GetMajorNumber(), dev.GetMinorNumber())
		} else {
			blkio.tags["device"] = "default"
		}
		for _, op := range ops {
			suffix := strings.ToLower(mesos.CgroupInfo_Blkio_Operation_name[int32(op)])
			warnIfNotSet(setIfNotNil(blkio.fields, fmt.Sprintf("io_serviced_%s", suffix),
				blkioGetter(throttling.GetIOServiced, op)))
			warnIfNotSet(setIfNotNil(blkio.fields, fmt.Sprintf("io_service_bytes_%s", suffix),
				blkioGetter(throttling.GetIOServiceBytes, op)))
		}

		results = append(results, blkio)
	}

	return results
}

// blkioGetter is a convenience method allowing us to unpick the nested
// blkio_value object. It returns a method which when invoked, returns the
// value of the field's operation type (passed in as param) returned by
// its parameter function
func blkioGetter(f func() []mesos.CgroupInfo_Blkio_Value, op mesos.CgroupInfo_Blkio_Operation) func() uint64 {
	return func() uint64 {
		for _, v := range f() {
			if v.GetOp() == op {
				return v.GetValue()
			}
		}
		return 0
	}
}

// cNetTrafficControlStatistics tags each set of traffic control statistics
// with the limiter ID
func cNetTrafficControlStatistics(tcs []mesos.TrafficControlStatistics) []measurement {
	var results []measurement

	for _, tc := range tcs {
		m := newMeasurement("net")
		m.tags["id"] = tc.GetID()
		warnIfNotSet(setIfNotNil(m.fields, "tx_backlog", tc.GetBacklog))
		warnIfNotSet(setIfNotNil(m.fields, "tx_bytes", tc.GetBytes))
		warnIfNotSet(setIfNotNil(m.fields, "tx_dropped", tc.GetDrops))
		warnIfNotSet(setIfNotNil(m.fields, "tx_over_limits", tc.GetOverlimits))
		warnIfNotSet(setIfNotNil(m.fields, "tx_packets", tc.GetPackets))
		warnIfNotSet(setIfNotNil(m.fields, "tx_qlen", tc.GetQlen))
		warnIfNotSet(setIfNotNil(m.fields, "tx_rate_bps", tc.GetRateBPS))
		warnIfNotSet(setIfNotNil(m.fields, "tx_rate_pps", tc.GetRatePPS))
		warnIfNotSet(setIfNotNil(m.fields, "tx_requeues", tc.GetRequeues))

		results = append(results, m)
	}

	return results
}

// cTags extracts relevant metadata from a Container object as a map of tags
func cTags(c agent.Response_GetContainers_Container) map[string]string {
	return map[string]string{"container_id": c.ContainerID.Value}
}

// cTS retrieves the timestamp from a Container object as a time rounded to the
// nearest second. If time is not available, we return now.
func cTS(c agent.Response_GetContainers_Container) (time.Time, bool) {
	if rs := c.GetResourceStatistics(); rs != nil {
		return time.Unix(int64(math.Trunc(c.ResourceStatistics.Timestamp)), 0), true
	}
	return time.Now(), false
}

// setIfNotNil runs get() and adds its value to a map, if not nil
func setIfNotNil(target map[string]interface{}, key string, get interface{}) error {
	var val interface{}
	var zero interface{}

	switch get.(type) {
	case func() uint32:
		val = get.(func() uint32)()
		zero = uint32(0)
		break
	case func() uint64:
		val = get.(func() uint64)()
		zero = uint64(0)
		break
	case func() int64:
		val = get.(func() int64)()
		zero = int64(0)
		break
	case func() float64:
		val = get.(func() float64)()
		zero = float64(0)
		break
	default:
		return fmt.Errorf("get function for key %s was not of a recognized type", key)
	}
	// Zero is nil for numeric types
	if val != zero {
		target[key] = val
	}
	return nil
}

// warnIfNotSet is a convenience method to log a warning whenever setIfNotNil
// did not succesfully complete
func warnIfNotSet(err error) {
	if err != nil {
		log.Printf("I! %s", err)
	}
}

// init is called once when telegraf starts
func init() {
	inputs.Add("dcos_containers", func() telegraf.Input {
		return &DCOSContainers{
			Timeout: internal.Duration{Duration: 10 * time.Second},
		}
	})
}
