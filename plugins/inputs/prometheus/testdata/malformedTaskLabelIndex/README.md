# Scenario: Malformed task label port index

- Given that there is a task with malformed port index task label
- When service discovery is attempted
- Then no metrics endpoints should be found
