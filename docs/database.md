# Database
- The database used here is a postgres database, running in a Docker container.
- Two main jobs of the database are:
    - Store the `products`, `users` data. This is assumed to be a transcations database
    - Store all airflow metadata(airflow db, dags, tasks, etc). This is assumed to be a metadata database

## Postgres
- The postgres service is using a docker volume to persist the data
- The init for creating the table is is located at `src/services/postgres_init`
    - When the postgres container is first started, the `init.sql` file runs create
    - This creates the `products` and `users` table
