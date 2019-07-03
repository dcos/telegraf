# Scenario: Various Network Modes

- Given that three tasks are running on the cluster
- And each has a labeled prometheus port
- And each uses a distinct method from among host, bridge, and container networking
- When service discovery is attempted
- Then three metrics endpoints should be found
- And each metric endpoint should be assembled from the task's IP address label
