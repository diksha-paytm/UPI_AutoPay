# Define the database connection to be used for this model.
connection: "starburst_connection"
include: "/views/**/*.view"
explore: users_who_never_activated_again {}
explore: users_who_revoked_mandate {}
explore: cc_datadump {}
explore: active_mandates {}
explore: cc_summary {}
explore: cc_create_error {}
explore: cc_1st_exec_error {}
explore: cc_revoke_error {}
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
