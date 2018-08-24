# Scenario: blkio

- Given that a task is running with a blkio device
- When container metrics are retrieved
- Then that task's container metrics should be present
- And blkio statistics should also be present
