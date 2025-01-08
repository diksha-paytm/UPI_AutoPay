# Define the database connection to be used for this model.
connection: "starburst_connection"
include: "/views/**/*.view"
explore: active_mandates {}
explore: users_who_never_activated_again {}
explore: users_who_revoked_mandate {}
