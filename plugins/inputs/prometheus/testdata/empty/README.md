# Scenario: Empty

- Given that no task are running on the cluster
- When service discovery is attempted
- Then no metrics endpoints should be found
