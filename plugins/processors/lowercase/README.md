# Lowercase Processor Plugin

The lowercase processor plugin ensures that metrics fields are lowercased. 

By default, metrics are coerced to lowercase. Optionally, metrics may be copied, so that the original metric is
preserved and a lowercase copy is also emitted. 

### Configuration:

```toml
# Coerce all metrics that pass through this filter to lowercase.
[[processors.lowercase]]
  ## Sends both Some_Metric and some_metric if true. 
  ## If false, sends only some_metric.
  # send_original = false
```

### Tags:

No tags are applied by this processor.
