# Run Test Syncs

1. Start mssql `docker compose up mssql frappe --build`
2. Use SQLTools and `TODO: Create Database` Connection to run `init.sql`
3. Switch to `Test DB` Connection in SQLTools and run `contact.sql` and the other `.sql` files
4. `docker compose up sync --build`
