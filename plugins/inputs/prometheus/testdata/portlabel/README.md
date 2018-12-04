# Scenario: Port Label

- Given that two tasks are running on the cluster
- And one task has a port metrics label
- When service discovery is attempted
- Then one metrics endpoint should be found
