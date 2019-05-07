# Scenario: Empty

- Given that no tasks are running on the cluster
- When container metrics are retrieved
- Then no container metrics should be present
