# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

mongo-backup is a Node.js CLI tool for MongoDB collection dumping with automatic monthly splitting and resume capabilities. It's published as `@rightson/mongo-backup` on npm and designed for both CLI usage via `npx @rightson/mongo-backup` and programmatic usage as a Node.js module.

## Architecture

### Core Components

**MongoDumper Class** (`lib/mongo-dumper.js`):
- Simplified business logic class using native MongoDB tools
- **Connection Management**: `buildConnectionUri()`, `connect()`, `disconnect()` - handles multiple connection methods (URI vs individual params), secure password prompting
- **Index Management**: `checkIndexExists()` - read-only index verification and warnings for optimal performance
- **Index Preservation**: `extractIndexes()`, `saveIndexes()` - automatic extraction and storage of all collection indexes
- **Date Analysis**: `getDateRange()`, `generateMonthlyRanges()` - analyzes collection date spans and creates monthly time ranges
- **State Management**: `loadState()`, `saveState()` - persistent resume functionality via `.dump-state.json`
- **Command Execution**: `executeCommand()` - handles mongodump process execution with real-time progress
- **Monthly Chunking**: `dumpMonth()` - uses mongodump with date queries for each month
- **Backup Cleanup**: `cleanBackedUpData()`, `validateBackupFile()`, `findBackupFiles()` - safe deletion of completed backups with validation
- **Orchestration**: `run()` - main method coordinating the entire dump process

**MongoRestorer Class** (`lib/mongo-restorer.js`):
- Simplified class handling chunk-based restoration using mongorestore  
- **Connection Management**: Same secure connection handling as MongoDumper
- **Index Restoration**: `loadIndexes()`, `restoreIndexes()` - automatic restoration of all saved indexes
- **Chunk Discovery**: `findDumpChunks()` - automated discovery of monthly dump chunk directories
- **Command Execution**: `executeCommand()` - handles mongorestore process execution with real-time progress
- **State Management**: `loadState()`, `saveState()` - persistent resume functionality via `.restore-state.json`
- **Selective Restore**: `restoreSpecificChunks()` - restore specific months or all chunks
- **Orchestration**: `run()` - main method coordinating the entire restore process

**CLI Interface** (`bin/cli.js`):
- Uses Commander.js for argument parsing
- Thin wrapper around MongoDumper and MongoRestorer classes
- Handles CLI-specific concerns (exit codes, error formatting)
- **Full Connection Support**: Supports both `--uri` and individual connection options (`-h`, `-p`, `--host`, `--username`, etc.)
- **Required Date Field**: `--date-field` is now required (no default) for explicit date filtering
- **Index Preservation Options**: `--skip-index-extraction`, `--skip-index-restoration` for manual index control
- **Backup Cleanup**: `clean` command for safely deleting already-backed-up months with validation
- **Selective Restore**: `--months` option for restoring specific monthly chunks
- **Non-Intrusive Design**: Tool only reads data and never modifies collections, indexes, or schema
- **Native Tool Integration**: Leverages mongodump/mongorestore for optimal performance

**Module Export** (`index.js`):
- Exports both MongoDumper and MongoRestorer classes for programmatic usage
- Allows `const { MongoDumper, MongoRestorer } = require('@rightson/mongo-backup')`

## Development Commands

### Primary Workflow (via manage.sh)
```bash
# Setup development environment
./manage.sh dev          # Installs deps + npm links for local testing

# Testing and validation
./manage.sh test         # Runs CLI validation tests
./manage.sh build        # Validates package structure

# Examples and help
./manage.sh examples     # Shows usage examples and creates sample scripts
```

### Publishing Workflow
```bash
./manage.sh publish-patch    # Bumps patch version and publishes
./manage.sh publish-minor    # Bumps minor version and publishes  
./manage.sh publish-major    # Bumps major version and publishes
```

### Git Operations
```bash
./manage.sh status           # Git status
./manage.sh commit "msg"     # Git add + commit
./manage.sh push            # Git push to origin
```

### Direct npm commands
```bash
npm start                   # Runs bin/cli.js directly
npm link                    # Links package locally for testing
```

## Key Technical Details

### Connection Architecture
The application supports two connection paradigms:
1. **URI-based**: Single `--uri` parameter with full connection string
2. **Component-based**: Individual `--host`, `--port`, `--username`, `--password`, etc. (currently only supported in core library, not CLI)

The `buildConnectionUri()` method handles both approaches and includes secure password prompting when username is provided without password.

### State-Based Resumption
- Uses `.dump-state.json` in output directory to track completed months
- `completedMonths[]` array prevents re-processing finished time ranges
- State file is automatically cleaned up on successful completion
- Resume works by filtering out already-completed monthly ranges before processing

### Memory-Agnostic Data Flow
1. **Index Optimization**: Automatic index creation/verification on date field for optimal query performance
2. **Analysis Phase**: `getDateRange()` queries collection min/max dates on specified field
3. **Range Generation**: `generateMonthlyRanges()` creates monthly time boundaries
4. **Adaptive Splitting**: `generateAdaptiveRanges()` splits large months (>1M docs) into smaller chunks
5. **State Filtering**: Removes already-completed months from processing queue
6. **True Streaming Processing**: For each range:
   - Manual cursor iteration (`cursor.next()`) with no internal buffering
   - One document at a time processing with immediate disk writes
   - Automatic range splitting prevents memory overflow
   - Real-time progress tracking with memory monitoring
   - Default gzip compression for space efficiency
   - Atomic file operations (cleanup on failure)

### File Output Structure
- **Chunk Directories**: `./dump-backup/YYYY-MM/` (monthly chunk directories)
- **Native BSON Files**: `./dump-backup/YYYY-MM/database/collection.bson.gz` (mongodump's native format)
- **Index File**: `{database}_{collection}_indexes.json` (contains all index definitions)
- **Default Output Directory**: `./dump-backup/`
- **Native BSON Support**: Uses mongodump's optimized BSON format with built-in compression
- **Default gzip compression**: Built-in mongodump compression (70-90% size reduction)
- **MongoRestorer** automatically handles chunk directories and mongorestore integration

## Performance Optimizations

### Simplified Native Tool Architecture
- **Native mongodump/mongorestore**: Leverages MongoDB's optimized native tools for maximum performance
- **Monthly Date Filtering**: Uses MongoDB query optimization with date range filters
- **Minimal Memory Usage**: mongodump handles memory management automatically 
- **Built-in Compression**: Native gzip compression with 70-90% space savings
- **Complete Index Preservation**: Automatic extraction and restoration of all indexes
- **Non-Intrusive Operation**: Only reads data, never modifies collections or creates indexes
- **Resume Capability**: State-based resumption tracks completed monthly chunks
- **Chunk-based Architecture**: Each month stored as separate directory for easy management

## Known Limitations/TODOs

1. **No Test Framework**: Package.json shows "no test specified" - only has basic validation in manage.sh

2. **Requires mongodump/mongorestore**: Users must have MongoDB database tools installed locally

## Development Context

- **Node.js**: Requires 14+ (specified in package.json engines)
- **Dependencies**: MongoDB driver (^6.19.0) + Commander.js (^11.1.0)
- **Package Structure**: Follows npm best practices with `bin/`, `lib/`, and main `index.js` export
- **Management Script**: `manage.sh` handles all development workflows (setup, testing, publishing)
- **Dual Architecture**: MongoDumper and MongoRestorer classes contain all business logic

### Recent Enhancements

**Version 1.7.0 - Simplified Native Tool Architecture**
- **Complete Rewrite**: Replaced complex 900+ line streaming implementation with 200-line mongodump wrapper
- **Native Tool Integration**: Uses mongodump/mongorestore for optimal performance and reliability
- **Required Date Field**: `--date-field` parameter is now required (no default) for explicit filtering
- **Chunk-based Structure**: Monthly dumps stored as `./dump-backup/YYYY-MM/` directories
- **Clear Progress Display**: Shows `[current/total]` progress for both dump and restore operations
- **Selective Restore**: New `--months` option to restore specific monthly chunks
- **Native BSON Support**: Real BSON format support via mongodump (removes previous JSON limitation)
- **Simplified Codebase**: 80% reduction in code complexity while maintaining all key features

**Version 1.3.0 - Backup Cleanup Feature**
- **Safe Backup Deletion**: New `clean` command for removing already-backed-up months
- **Validation Before Deletion**: Ensures backup files are complete and exist before deletion
- **State File Integration**: Cross-references with `.dump-state.json` to verify completion status
- **Selective Cleanup**: Target specific months or clean all completed backups
- **Safety Features**: Dry-run mode, confirmation prompts, integrity validation
- **Error Prevention**: Avoids attempting to delete non-existent or already-removed files
- **Automatic State Updates**: Updates state file after successful deletions

**Version 1.1.0 - Complete Index Preservation**
- **Automatic Index Extraction**: All collection indexes automatically saved during dumps
- **Automatic Index Restoration**: All saved indexes automatically recreated during restores  
- **Index File Storage**: `{database}_{collection}_indexes.json` contains complete index definitions
- **Manual Override Options**: `--skip-index-extraction` and `--skip-index-restoration` flags
- **Comprehensive Coverage**: Supports all MongoDB index types (compound, text, 2dsphere, TTL, etc.)
- **Error Resilience**: Graceful handling of index creation failures and duplicates

**Previous Enhancements**
- **Performance**: Optimized for 500GB+ collections with intelligent defaults
- **Index Management**: Automatic index creation with advanced options support
- **Compression**: Default compression with space savings up to 90%
- **CLI Completeness**: Full connection parameter support matching MongoDB tools
- **User Experience**: Intelligent warnings and suggestions for optimal performance

The codebase follows a clean class-based architecture making it straightforward to understand and extend while supporting both CLI and programmatic usage patterns. The new index preservation system ensures complete data fidelity during dump/restore cycles, making it production-ready for critical database operations.