# Scenario: Fresh

- Given that a task is running on the cluster
- And that task's information is _not_ cached
- When container metrics are retrieved
- Then that task's container metrics should be present
- And that task's tags should be present
