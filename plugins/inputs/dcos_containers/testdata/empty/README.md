# Scenario: Empty

- Given that no task are running on the cluster
- When container metrics are retrieved
- Then no container metrics should be presnet
