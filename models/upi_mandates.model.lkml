# Define the database connection to be used for this model.
connection: "starburst_connection"
include: "/views/**/*.view"
explore: active_mandates {}
explore: overall_count {}
explore: overall_sr {}
explore: creations_sr {}
explore: 1st_exec_sr {}
explore: recurring_exec_sr {}
explore: revoke_sr {}
explore: payer_revoke_sr {}
explore: creations_sr_across_mode {}
explore: creations_count {}
explore: 1st_exec_count {}
explore: recurring_exec_count {}
explore: payer_revoke_count {}
explore: payee_revoke_count {}
explore: pdn_count {}
explore: creations_count_across_mode {}
explore: creations_error_count_handle_wise {}
explore: overall_1st_exec_error_count {}
explore: 1st_exec_error_count_handle_wise {}
explore: overall_recurring_exec_error_count {}
explore: recurring_exec_error_count_handle_wise {}
explore: overall_revoke_error_count {}
explore: revoke_error_count_handle_wise {}
explore: overall_creations_error_count {}
explore: creations_os_wise_count {}
explore: creations_sr_os_wise {}
explore: mandate_dtu {}
explore: recurring_mandates_dtu {}
explore: rm_creations_dtu {}
explore: rm_1st_exec_dtu {}
explore: rm_recurring_exec_dtu {}
explore: rm_revoke_dtu {}
explore: upi_mandate_dtu_success {}
explore: cc_top_merchants {}
explore: cc_active_mandates {}
explore: cc_summary {}
explore: cc_failure {}
explore: cc_success{}
explore: cc_sr {}
explore: cc_datadump {}
explore: cc_create_error {}
explore: cc_1st_exec_error {}
explore: cc_recurring_exec_error {}
explore: cc_revoke_error {}
