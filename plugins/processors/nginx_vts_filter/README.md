# Nginx VTS Filter Processor Plugin

The `nginx_vts_filter` processor converts specific measurements, tags.

### Configuration:

```toml
[[processors.nginx_vts_filter]]

  [[processors.nginx_vts_filter.convert]]
    measurement = "nginx_vts_filter_requests_total"
    tags_delimiter = ","
    key_value_delimiter = "="

  [[processors.nginx_vts_filter.convert]]
    measurement = "nginx_vts_filter_bytes_total"
    tags_delimiter = ","
    key_value_delimiter = "="

  [[processors.nginx_vts_filter.convert]]
    measurement = "nginx_vts_filter_request_seconds"
    tags_delimiter = ","
    key_value_delimiter = "="

```

### Tags:

The `filter` and `filter_name` tags which contain key-value pairs like `upstream=Bouncer,status=200` are unmarshalled by this processor, so that they replace the initial tags.

Measurement names are also altered in a particular fashion explained below.

`*_x_y_*{filter="a=*,b=*", filter_name="c=*,d=*"}` becomes `*_a_c_*{a=*,b=*,c=*,d=*}`

The delimter between nginx_vts_filter tags is ',' by default. The key value pairs of each tag are separated by the '=' delimiter by default.

### Example processing:

```diff
- nginx_vts_filter_requests_total{filter="upstream=Bouncer,backend=,status=401",filter_name="client=Mesos/1.8.0 authorizer (master)"}
+ nginx_upstream_client_requests_total{upstream="Bouncer",backend="",status="401",client="Mesos/1.8.0 authorizer (master)"}
```
