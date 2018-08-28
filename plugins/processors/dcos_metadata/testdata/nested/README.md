# Scenario: Nested

- Given that a task is running on the cluster
- And that task has a nested container
- When container metrics are retrieved
- Then that task's nested container metrics should be present
- And that task's tags should be present
