# ============================================================
# Outputs — Flink statement IDs for verification
# ============================================================

output "flink_statement_ids" {
  description = "IDs of created Flink statements"
  value = {
    register_udf      = confluent_flink_statement.register_udf.id
    flatten_eoi       = confluent_flink_statement.flatten_eoi.id
    route_bentech         = confluent_flink_statement.route_bentech.id
    transform_workday = confluent_flink_statement.transform_workday.id
  }
}
