# Scenario: net_snmp_statistics

- Given that a task is running that emits net snmp statistics
- When container metrics are retrieved
- Then that task's container metrics should be present
- And net snmp statistics should also be present
