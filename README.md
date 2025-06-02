# dm-data-consistency-checker
Verifies referential integrity across masked datasets. Useful for ensuring relationships between tables remain valid after masking, preventing data corruption and analysis errors. Uses SQL queries to check foreign key constraints. - Focused on Tools designed to generate or mask sensitive data with realistic-looking but meaningless values

## Install
`git clone https://github.com/ShadowStrikeHQ/dm-data-consistency-checker`

## Usage
`./dm-data-consistency-checker [params]`

## Parameters
- `-h`: Show help message and exit
- `--db_path1`: Path to the first SQLite database file.
- `--db_path2`: No description provided
- `--table_name`: Name of the table to check for referential integrity.
- `--foreign_key_column`: Name of the foreign key column in the table.
- `--parent_table`: Name of the parent table referenced by the foreign key.
- `--parent_key_column`: Name of the primary key column in the parent table.

## License
Copyright (c) ShadowStrikeHQ
