# Scenario: Port Label

- Given that three tasks are running on the cluster
- And one task has a port metrics label
- And another one task has a port metrics label with a custom route
- When service discovery is attempted
- Then one metrics endpoint should be found and default to /metrics
- And one  metrics endpoint should be found and use its custom route
