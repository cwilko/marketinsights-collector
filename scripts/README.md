# Database Setup Scripts

## setup_database.sql

Creates the PostgreSQL database, user, and permissions needed for the econometrics pipeline.

### Usage

#### Default values (recommended for development):
```bash
psql -U postgres \
     -v db_name="econometrics" \
     -v db_user="econometrics_user" \
     -v db_password="econometrics_password" \
     -f scripts/setup_database.sql
```

#### Using environment variables:
```bash
export ECON_DB_NAME=my_econometrics_db
export ECON_DB_USER=my_user  
export ECON_DB_PASSWORD=secure_password123

psql -U postgres \
     -v db_name="$ECON_DB_NAME" \
     -v db_user="$ECON_DB_USER" \
     -v db_password="$ECON_DB_PASSWORD" \
     -f scripts/setup_database.sql
```

#### With remote PostgreSQL:
```bash
psql -h your_postgres_host -p 5432 -U postgres \
     -v db_name="econometrics" \
     -v db_user="econ_user" \
     -v db_password="your_secure_password" \
     -f scripts/setup_database.sql
```


### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ECON_DB_NAME` | `econometrics` | Database name |
| `ECON_DB_USER` | `econometrics_user` | Database user |
| `ECON_DB_PASSWORD` | `econometrics_password` | Database password |

### Output

The script will display the `DATABASE_URL` to add to your `.env` file:
```
DATABASE_URL=postgresql://econometrics_user:econometrics_password@localhost:5432/econometrics
```

### Requirements

- PostgreSQL server running
- Superuser access (typically `postgres` user)  
- `psql` command-line client installed and in PATH
- For Makefile usage: ensure `psql` is available in your shell environment

### What it does

1. Creates database if it doesn't exist
2. Creates user if it doesn't exist
3. Grants all privileges on database to user
4. Sets up schema permissions
5. Configures default privileges for future tables

## cleanup_database.sql

**⚠️ WARNING: This permanently deletes the database and all data!**

Removes the PostgreSQL database and user created by the setup script.

### Usage

#### Default cleanup:
```bash
psql -U postgres \
     -v db_name="econometrics" \
     -v db_user="econometrics_user" \
     -f scripts/cleanup_database.sql
```

#### Using environment variables:
```bash
export ECON_DB_NAME=my_econometrics_db
export ECON_DB_USER=my_user

psql -U postgres \
     -v db_name="$ECON_DB_NAME" \
     -v db_user="$ECON_DB_USER" \
     -f scripts/cleanup_database.sql
```


### Safety Features

- **Clear warnings** - Shows exactly what will be deleted
- **Connection termination** - Safely closes all connections before deletion
- **Variable support** - Uses same variable names as setup script

### What it does

1. Displays warning showing what will be deleted
2. Terminates all connections to the target database
3. Drops the database if it exists
4. Drops the user if it exists
5. Confirms successful cleanup

### Example Session

```
⚠️  WARNING: This will permanently delete:
Database name: econometrics
User name: econometrics_user

All data will be lost permanently!

Proceeding with deletion...

✅ Database cleanup completed successfully!

## update_cpi_calculations.sql

Retrospectively calculates Year-over-Year and Month-over-Month changes for existing CPI records in the database. This populates historical inflation data that was previously NULL.

### Usage

#### Using the Python wrapper (recommended):
```bash
export DATABASE_URL="postgresql://user:password@host:port/database"
python scripts/run_cpi_retrospective_update.py
```

#### Direct SQL execution:
```bash
psql $DATABASE_URL -f scripts/update_cpi_calculations.sql
```

### What it does

1. **Year-over-Year Calculation**: Updates `year_over_year_change` column using 12-month intervals
2. **Month-over-Month Calculation**: Updates `month_over_month_change` column using 1-month intervals  
3. **Timestamp Update**: Sets `updated_at` for all modified records
4. **Summary Report**: Shows count of records updated with calculations

### Requirements

- Existing CPI data in `consumer_price_index` table
- At least 12+ months of data for meaningful YoY calculations
- Valid DATABASE_URL environment variable

### Expected Results

- ~235 records updated with YoY data (requires 12-month historical context)
- ~246 records updated with MoM data (requires 1-month historical context) 
- YoY inflation range typically -2% to +9% for recent decades
- Enables inflation charts and metrics in the Streamlit dashboard

### Safety Features

- **Idempotent**: Safe to run multiple times - will recalculate existing values
- **Transactional**: All updates committed together or rolled back on error
- **Non-destructive**: Only updates calculated columns, preserves original CPI values