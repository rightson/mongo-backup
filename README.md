# mongo-backup

A high-performance MongoDB collection dumper optimized for large datasets (500GB+) with automatic monthly splitting, intelligent compression, and resume capabilities.

## Features

- 🗓️ **Automatic Monthly Splitting**: Splits your collection into separate files by month
- ⚡ **Resume Capability**: Can resume from any interrupted point without data loss
- 📊 **Progress Tracking**: Real-time progress reporting with document counts
- 🗜️ **Smart Compression**: Default gzip compression with 70-90% space savings
- 🚀 **Memory Efficient**: Optimized streaming with large collection batch sizes (50K default)
- 📈 **Complete Index Preservation**: Automatically extracts and restores all indexes during dump/restore cycles
- 🔧 **Easy CLI**: Full-featured command-line interface with MongoDB-compatible options
- 📁 **Organized Output**: Clean file naming with year-month format
- 📦 **Dual Usage**: Works as both CLI tool and Node.js module
- 🎯 **Large Dataset Optimized**: Designed for 500GB+ collections with intelligent defaults

## Installation

### Option 1: NPX (Recommended)

```bash
# Run directly without installation
npx @rightson/mongo-backup -d myDatabase -c myCollection
```

### Option 2: Global Installation

```bash
# Install globally
npm install -g @rightson/mongo-backup

# Then use anywhere
mongo-backup -d myDatabase -c myCollection
```

### Option 3: Local Development

```bash
# Clone and setup
git clone https://github.com/rightson/mongo-backup.git
cd mongo-backup
./manage.sh setup
```

## Usage

### CLI Usage

```bash
# Basic dump (indexes automatically preserved)
npx @rightson/mongo-backup dump -d myDatabase -c myCollection

# Basic restore (indexes automatically restored)
npx @rightson/mongo-backup restore -d myDatabase -c myCollection

# With MongoDB connection options (compression enabled by default)
npx @rightson/mongo-backup dump \
  --uri "mongodb://localhost:27017" \
  --database "ecommerce" \
  --collection "orders"

# With authentication and auto-index creation
npx @rightson/mongo-backup dump \
  --uri "mongodb://user@db.example.com/myapp" \
  --database "production" \
  --collection "users" \
  --date-field "createdAt" \
  --output-dir "./backups" \
  --create-index "createdAt"
```

### Programmatic Usage

```javascript
const { MongoDumper, MongoRestorer } = require('@rightson/mongo-backup');

// Dump with automatic index extraction (default behavior)
const dumper = new MongoDumper({
  uri: 'mongodb://localhost:27017',
  database: 'myapp',
  collection: 'users',
  dateField: 'createdAt',
  outputDir: './backup',
  createIndex: 'createdAt',          // Auto-create index
  compress: true,                    // Default: true
  batchSize: 50000,                  // Default: 50000
  skipIndexExtraction: false         // Default: false (indexes preserved)
});

await dumper.run();

// Restore with automatic index restoration (default behavior)
const restorer = new MongoRestorer({
  uri: 'mongodb://localhost:27017',
  database: 'myapp',
  collection: 'users',
  inputDir: './backup',
  batchSize: 25000,                  // Default: 25000
  skipIndexRestoration: false        // Default: false (indexes restored)
});

await restorer.run();
```

## CLI Options

### Key Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--uri` | `-u` | MongoDB connection URI | `mongodb://localhost:27017` |
| `--database` | `-d` | Database name | `test` |
| `--collection` | `-c` | Collection name | **Required** |
| `--date-field` | `-f` | Date field for monthly splitting | `createdAt` |
| `--output-dir` / `--input-dir` | `-o` / `-i` | Output/Input directory | `./dump-extra` |
| `--batch-size` | `-b` | Documents per batch | `50000` (dump), `25000` (restore) |
| `--compress` / `--no-compress` | `-z` | Enable/disable gzip compression | `true` |
| `--drop` | | Drop collection before restore | `false` |

## Examples

### Complete Dump and Restore

```bash
# Dump collection with automatic index preservation
npx @rightson/mongo-backup dump \
  --uri "mongodb+srv://user:pass@cluster.mongodb.net/" \
  --database "production" \
  --collection "transactions"

# Restore collection with automatic index restoration
npx @rightson/mongo-backup restore \
  --uri "mongodb+srv://user:pass@cluster.mongodb.net/" \
  --database "production" \
  --collection "transactions"
```

### Custom Date Field and Output

```bash
npx @rightson/mongo-backup dump \
  --uri "mongodb://localhost:27017" \
  --database "analytics" \
  --collection "events" \
  --date-field "timestamp" \
  --output-dir "./analytics-backup"
```

### Performance Optimization

```bash
# For optimal performance, ensure indexes exist before dumping
# mongo-backup is completely non-intrusive and never modifies your collection
npx @rightson/mongo-backup dump \
  --uri "mongodb://localhost:27017" \
  --database "ecommerce" \
  --collection "orders"
```

### Batch Size Tuning

```bash
# Large documents - smaller batches
npx @rightson/mongo-backup dump \
  -d "logs" -c "detailed_logs" \
  --batch-size 10000

# Small documents - larger batches  
npx @rightson/mongo-backup dump \
  -d "warehouse" -c "products" \
  --batch-size 75000
```

## Resume Functionality

The utility automatically tracks progress in a `.dump-state.json` file. If interrupted:

1. Simply run the same command again
2. It will automatically resume from where it left off
3. Already completed months are skipped
4. Progress is maintained across restarts

## Output Structure

```
./dump-extra/
├── .dump-state.json              # Progress tracking (auto-deleted on completion)
├── mydb_mycoll_indexes.json     # All collection indexes (auto-extracted)
├── mydb_mycoll_2023-01.jsonl    # January 2023 data (JSONL format)
├── mydb_mycoll_2023-02.jsonl.gz # February 2023 data (compressed)
├── mydb_mycoll_2023-03.jsonl.gz # March 2023 data (compressed)
└── ...
```

## File Naming Convention

Files are named as: `{database}_{collection}_{YYYY-MM}.{format}{.gz}`

Examples:
- `ecommerce_orders_2023-12.jsonl`
- `logs_events_2024-01.jsonl.gz`  
- `ecommerce_orders_indexes.json` (index definitions)

## JSONL Format Benefits

mongo-backup now uses JSONL (JSON Lines) format instead of JSON arrays, providing significant advantages for large collections:

- **Memory Efficient**: Process one document at a time, not entire files
- **Interruption Safe**: Partial files remain valid (just fewer lines)
- **Streaming**: No need to load entire dataset into memory
- **Faster**: No pretty-printing overhead, optimized for processing

## Index Preservation (New in 1.1.0)

mongo-backup automatically preserves all collection indexes during dump and restore operations. All custom indexes (unique, compound, text, TTL, etc.) are extracted during dumps and automatically recreated during restores, ensuring complete data fidelity.

## Performance Tips

1. **Indexing**: For optimal performance, ensure your date field is indexed:
   ```javascript
   db.mycollection.createIndex({ "createdAt": 1 })
   ```
   mongo-backup will warn if the index is missing but never modifies your collection

2. **Batch Size**: Adjust based on document size and available memory:
   - Large documents: Use smaller batch size (10000-25000) 
   - Small documents: Use larger batch size (50000-100000)

3. **Compression**: Enabled by default for space savings (disable with --no-compress if needed)

4. **Network**: Run closer to your MongoDB instance for faster transfers

5. **Non-Intrusive**: mongo-backup only reads data and never modifies your collection or indexes

## Large Collection Optimization (500GB+)

mongo-backup is specifically optimized for large datasets with intelligent defaults:

### **Automatic Optimizations**
- **Smart Batch Sizes**: 50,000 documents per batch for dumps, 25,000 for restores
- **Default Compression**: ~70-90% space reduction with gzip compression
- **Index Preservation**: Automatically extracts and preserves all collection indexes
- **Non-Intrusive**: Only reads data, never modifies collections or creates indexes
- **Memory Efficient**: Streaming architecture prevents memory overflow

### **Expected Performance** (500GB collection)
- **Processing Time**: 2-8 hours depending on document complexity and hardware
- **Disk Usage**: ~50-150GB with compression (vs 500GB uncompressed)
- **Memory Usage**: <100MB (consistent streaming)
- **Network Efficiency**: Optimized batch sizes reduce connection overhead

### **Best Practices for Large Collections**
```bash
# Optimal dump command for 500GB+ collections
npx @rightson/mongo-backup dump \
  --uri "your-connection-string" \
  --database "your-db" \
  --collection "your-collection" \
  --batch-size 50000

# Restore with automatic index recreation
npx @rightson/mongo-backup restore \
  --uri "your-connection-string" \
  --database "your-db" \
  --collection "your-collection" \
  --batch-size 25000

# For very large documents, reduce batch size
npx @rightson/mongo-backup dump \
  --database "logs" \
  --collection "detailed-logs" \
  --batch-size 10000
```

## Development

### Management Commands

```bash
# Setup development environment
./manage.sh setup

# Show usage examples
./manage.sh examples

# Run tests
./manage.sh test

# Publish new version
./manage.sh publish-patch
```

### Project Structure

```
mongo-backup/
├── bin/cli.js              # CLI executable
├── lib/mongo-dumper.js     # Core MongoDumper class
├── lib/mongo-restorer.js   # Core MongoRestorer class  
├── index.js                # Main module export
├── manage.sh               # Development management script
└── package.json            # Package configuration
```

## Error Handling

The utility includes robust error handling:

- Network interruptions: Automatically detected and reported
- Disk space issues: Partial files are cleaned up
- Memory issues: Streaming prevents memory overflow
- State corruption: Can be manually reset by deleting `.dump-state.json`

## Requirements

- Node.js 14+ 
- MongoDB 3.6+
- Sufficient disk space for output files (with compression: ~15-30% of original collection size)
- Network access to MongoDB instance
- For 500GB+ collections: Ensure proper indexes exist on date fields

## Troubleshooting

### Collection is Empty Error
- Check that the collection name is correct
- Verify the date field exists in documents
- Ensure MongoDB connection is working

### Resume Not Working
- Delete `.dump-state.json` to force a fresh start
- Check file permissions in output directory
- Verify the same parameters are used when resuming

### Out of Memory
- Reduce batch size (`-b 1000`)
- Enable compression (`-z`)
- Check available system memory

### Slow Performance
- Ensure indexes exist on your date field before dumping
- Increase batch size for small documents (--batch-size 75000)
- Run closer to MongoDB server
- Ensure compression is enabled (default) for faster I/O

## Contributing

Feel free to submit issues and enhancement requests at https://github.com/rightson/mongo-backup/issues

## License

MIT License - feel free to use in your projects.