# Scenario: Unrelated

- Given that no tasks are running on the cluster
- And a metric is received
- And that metric has no container_id tag
- Then that metric should no be updated
