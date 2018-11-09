# DC/OS Metadata Plugin

The DC/OS metdata processor plugin tracks state on the mesos agent, and decorates every metric which passes through it
from the [dcos_container](../../input/dcos_container) and [dcos_statsd](../../input/dcos_statsd) plugins with 
appropriate metadata in the form of DC/OS primitives. 


### Configuration

```toml
# Associate metadata with dcos-related metrics
[[processors.dcos_metadata]]
  ## The URL of the mesos agent
  mesos_agent_url = "http://$NODE_PRIVATE_IP:5051"
  ## The period after which requests to mesos agent should time out
  timeout = "10s"
  ## The minimum period between requests to the mesos agent
  rate_limit = "5s"
  ## List of labels to always add to each metric as tags
  whitelist = []
  ## List of prefixes a label should have in order to be added
  ## to each metric as tags; the prefix is stripped from the
  ## label when tagging
  whitelist_prefix = []
  ## Optional IAM configuration
  # ca_certificate_path = "/run/dcos/pki/CA/ca-bundle.crt"
  # iam_config_path = "/run/dcos/etc/dcos-telegraf/service_account.json"
```

### Tags:

This process adds the following tags to any metric with a container_id tag set:

 - `task_name` - the name of the task associated with this container
 - `executor_name` - the name of the executor which started the task associated
                     with this container
 - `service_name` - the name of the service (mesos framework) which scheduled 
                    the task associated with this container

Additionally, any task labels which are prefixed with strings included in the configurable whitelist of prefixes
(`whitelist_prefix`) are added to each metric as a tag. For example, the application configuration would have every
metric associated with it decorated with a `FOO=bar` tag if `whitelist_prefix` was configured to include
`DCOS_METRICS_`. Any task labels that are included in the configurable whitelist (`whitelist`) are also added to each
metric as a tag.

```
{
  "id": "some-task",
  "cmd": "sleep 123",
  "labels": {
    "HAPROXY_V0_HOST": "http://www.example.com",
    "DCOS_METRICS_FOO": "bar"
  }
}
```
