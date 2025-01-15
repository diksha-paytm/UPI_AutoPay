# Define the database connection to be used for this model.
connection: "starburst_connection"
include: "/views/**/*.view"
explore: users_who_never_activated_again {}
explore: users_who_revoked_mandate {}
explore: creations_count {}
explore: exec_revokes_pdn_count {}
explore: creations_error_count {}
explore: revokes_error_count {}
explore: 1st_exec_error_count {}
explore: cc_mandates {}
explore: 1_exec_error_count {}
explore: active_mandates {}
