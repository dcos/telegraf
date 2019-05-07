# Scenario: net_traffic_control_statistics

- Given that a task is running that emits net traffic control statistics
- When container metrics are retrieved
- Then that task's container metrics should be present
- And net traffic control statistics should also be present
