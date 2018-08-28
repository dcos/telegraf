# Scenario: No Executor

- Given that a task is running on the cluster
- And the task has no executor ID
- When container metrics are retrieved
- Then that task's container metrics should be present
- And those metrics should not be tagged with executor_name
