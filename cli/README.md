# SurrealKV CLI

A command-line tool for managing and analyzing SurrealKV storage. This CLI provides utilities for pruning old versions, collecting statistics, and repairing corrupted storage.

## Installation

```bash
cargo install skv-cli
```

## Usage

```bash
skv-cli -d <database_path> <command> [options]
```

## Commands

### Analyze Version Distribution

Lists the top N keys with the most versions.

```bash
# Show top 10 keys in table format
skv-cli -d /path/to/db top-versions

# Show top 20 keys in JSON format
skv-cli -d /path/to/db top-versions -n 20 --format json
```

Options:
- `-n, --count <N>`: Number of top keys to show (default: 10)
- `-f, --format <FORMAT>`: Output format (table/json) (default: table)

### Prune Specific Keys

Delete all versions except the latest for specified keys and migrate to a new location.

```bash
# Prune using comma-separated keys
skv-cli -d /path/to/db prune-keys -k key1,key2,key3 --destination /path/to/dest

# Prune using a file containing keys
skv-cli -d /path/to/db prune-keys --keys-file keys.txt --destination /path/to/dest

# Dry run to see what would be pruned
skv-cli -d /path/to/db prune-keys -k key1,key2 --destination /path/to/dest
```

Options:
- `-k, --keys <KEYS>`: Comma-separated list of keys to prune
- `--keys-file <FILE>`: File containing keys to prune (one per line)
- `-d, --destination <DIR>`: Destination directory for pruned database

### Prune All Keys

Delete all versions except the latest for all keys and migrate to a new location.

```bash
# Prune all keys
skv-cli -d /path/to/db prune-all --destination /path/to/dest

# Dry run
skv-cli -d /path/to/db prune-all --destination /path/to/dest
```

Options:
- `-d, --destination <DIR>`: Destination directory for pruned database

### Database Statistics

Show comprehensive statistics about the database.

```bash
skv-cli -d /path/to/db stats
```

Statistics include:
- Total number of keys
- Total versions
- Maximum key/value sizes
- Average key/value sizes
- Total database size

### Repair Database

Repair corrupted commit log segments.

```bash
# Repair in-place (creates backup automatically)
skv-cli -d /path/to/db repair
```

## Examples

1. Prune specific keys and migrate:
```bash
# Create a keys.txt file
echo "key1
key2
key3" > keys.txt

# Prune using the file
skv-cli -d /path/to/db prune-keys --keys-file keys.txt --destination /path/to/dest
```

2. Prune all keys retaining only latest versions:
```bash
skv-cli -d /path/to/db prune-all --destination /path/to/pruned_db
```

3. Get database statistics:
```bash
skv-cli -d /path/to/db stats
```

4. Repair corrupted database:
```bash
skv-cli -d /path/to/db repair
```

## Understanding Version Management

- The CLI uses transaction IDs (which are sequential) to determine the latest version of each key
- When pruning, only the version with the highest transaction ID is kept
- The manifest folder is automatically copied during pruning operations to maintain database consistency
- Corrupted segments are handled gracefully with proper error reporting

## Notes

- Always backup your database before using pruning or repair operations
- The CLI preserves the manifest folder during migrations
- Progress bars indicate operation status for long-running commands
- JSON output is available for easy integration with other tools
- Dry run options are available for pruning operations to preview changes

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.