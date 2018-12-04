# Scenario: Task Label

- Given that two tasks are running on the clusterAAj
- And one task has a metrics label (not at the port level)
- When service discovery is attempted
- Then one metrics endpoint should be found
