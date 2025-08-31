#!/usr/bin/env node

const { program } = require('commander');
const { MongoDumper } = require('../lib/mongo-dumper');
const { MongoRestorer } = require('../lib/mongo-restorer');

// CLI Setup
program
    .name('mongo-backup')
    .description('MongoDB collection dumper and restorer with monthly splitting and resume capability')
    .version('1.0.0');

// Dump command
program
    .command('dump')
    .description('Dump MongoDB collection with monthly splitting')
    .option('-u, --uri <uri>', 'MongoDB connection URI', 'mongodb://localhost:27017')
    .option('-h, --host <host>', 'MongoDB host', 'localhost')
    .option('-p, --port <port>', 'MongoDB port', '27017')
    .option('--username <username>', 'MongoDB username')
    .option('--password <password>', 'MongoDB password')
    .option('--authentication-database <db>', 'Authentication database')
    .option('-d, --database <name>', 'Database name', 'test')
    .option('-c, --collection <name>', 'Collection name')
    .requiredOption('-f, --date-field <field>', 'Date field for splitting (required)')
    .option('-o, --output-dir <dir>', 'Output directory', './dump-backup')
    .option('-b, --batch-size <size>', 'Batch size for querying (capped at 10K for 10M+ documents)', '50000')
    .option('-z, --compress', 'Compress output files with gzip (default: true)')
    .option('--no-compress', 'Disable compression')
    .option('--format <format>', 'Output format (json|bson)', 'json')
    .option('--skip-index-extraction', 'Skip automatic index extraction and saving (default: false)')
    .option('--enable-gc', 'Enable aggressive garbage collection for large datasets')
    .option('--debug-listeners', 'Enable debug logging for event listener counts')
    .action(async (options) => {
        if (!options.collection) {
            console.error('Error: Collection name is required');
            process.exit(1);
        }

        // Parse numeric options and handle compression default
        const parsedOptions = {
            ...options,
            batchSize: parseInt(options.batchSize),
            port: parseInt(options.port),
            compress: options.noCompress ? false : (options.compress !== undefined ? options.compress : true),
            skipIndexExtraction: options.skipIndexExtraction || false,
            debugListeners: options.debugListeners || false
        };

        // Ultra-conservative settings for complex documents (4000+ keys)
        if (parsedOptions.batchSize <= 10) {
            console.log('🔧 Detected ultra-small batch size - enabling aggressive memory optimizations');
            
            // Set Node.js memory optimization flags if not already set
            if (!process.env.NODE_OPTIONS || !process.env.NODE_OPTIONS.includes('--expose-gc')) {
                console.log('⚠️  For optimal memory management with complex documents, restart with:');
                console.log('   NODE_OPTIONS="--expose-gc --max-old-space-size=4096" npx @rightson/mongo-backup dump ...');
                console.log('');
            }
            
            // Force enable GC if available
            if (global.gc) {
                console.log('✓ Garbage collection enabled');
            } else if (options.enableGc) {
                console.log('⚠️  GC not available. Use NODE_OPTIONS="--expose-gc" to enable');
            }
        }

        const dumper = new MongoDumper(parsedOptions);

        try {
            await dumper.run();
            console.log('\n✓ Dump completed!');
            process.exit(0);
        } catch (error) {
            console.error('\n✗ Dump failed:', error.message);
            process.exit(1);
        }
    });

// Clean command
program
    .command('clean')
    .description('Delete already-backed-up months after validation')
    .option('-d, --database <name>', 'Database name', 'test')
    .option('-c, --collection <name>', 'Collection name')
    .option('-o, --output-dir <dir>', 'Output directory containing backup files', './dump-backup')
    .option('-m, --months <months>', 'Specific months to delete (comma-separated, e.g., "2023-01,2023-02")')
    .option('--dry-run', 'Show what would be deleted without actually deleting')
    .option('--no-confirm', 'Skip confirmation prompt (use with caution)')
    .option('-z, --compress', 'Assume compressed files (.gz)')
    .option('--no-compress', 'Assume uncompressed files')
    .option('--format <format>', 'Backup file format (json|bson)', 'json')
    .action(async (options) => {
        if (!options.collection) {
            console.error('Error: Collection name is required');
            process.exit(1);
        }

        // Parse months if provided
        const months = options.months ? options.months.split(',').map(m => m.trim()) : null;

        // Parse options
        const parsedOptions = {
            ...options,
            compress: options.noCompress ? false : (options.compress !== undefined ? options.compress : true),
            confirmDelete: options.confirm !== false // Default to true unless --no-confirm
        };

        const dumper = new MongoDumper(parsedOptions);

        try {
            const result = await dumper.cleanBackedUpData({
                months,
                confirmDelete: parsedOptions.confirmDelete,
                dryRun: options.dryRun
            });

            if (options.dryRun) {
                console.log(`\n✓ Dry run completed: ${result.dryRun?.length || 0} files would be deleted`);
            } else if (result.cancelled) {
                console.log('\n✓ Clean operation cancelled');
            } else {
                console.log(`\n✓ Clean completed: ${result.deleted.length} files deleted`);
                if (result.errors.length > 0) {
                    console.log(`   ${result.errors.length} errors occurred`);
                    process.exit(1);
                }
            }
            
            process.exit(0);
        } catch (error) {
            console.error('\n✗ Clean failed:', error.message);
            process.exit(1);
        }
    });

// Restore command
program
    .command('restore')
    .description('Restore MongoDB collection from monthly dump chunks')
    .option('-u, --uri <uri>', 'MongoDB connection URI', 'mongodb://localhost:27017')
    .option('-h, --host <host>', 'MongoDB host', 'localhost')
    .option('-p, --port <port>', 'MongoDB port', '27017')
    .option('--username <username>', 'MongoDB username')
    .option('--password <password>', 'MongoDB password')
    .option('--authentication-database <db>', 'Authentication database')
    .option('-d, --database <name>', 'Database name', 'test')
    .option('-c, --collection <name>', 'Collection name')
    .option('-i, --input-dir <dir>', 'Input directory containing dump chunks', './dump-backup')
    .option('-m, --months <months>', 'Specific months to restore (comma-separated, e.g., "2024-01,2024-03")')
    .option('--drop', 'Drop collection before restore')
    .option('--skip-index-restoration', 'Skip automatic index restoration (default: false)')
    .action(async (options) => {
        if (!options.collection) {
            console.error('Error: Collection name is required');
            process.exit(1);
        }

        // Parse months if provided
        const months = options.months ? options.months.split(',').map(m => m.trim()) : null;

        // Parse numeric options
        const parsedOptions = {
            ...options,
            port: parseInt(options.port),
            skipIndexRestoration: options.skipIndexRestoration || false
        };

        const restorer = new MongoRestorer(parsedOptions);

        try {
            if (months) {
                await restorer.restoreSpecificChunks(months);
                console.log(`\n✓ Selective restore completed for months: ${months.join(', ')}!`);
            } else {
                await restorer.run();
                console.log('\n✓ Restore completed!');
            }
            process.exit(0);
        } catch (error) {
            console.error('\n✗ Restore failed:', error.message);
            process.exit(1);
        }
    });

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('\n\nReceived SIGINT. Graceful shutdown...');
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\n\nReceived SIGTERM. Graceful shutdown...');
    process.exit(0);
});

if (require.main === module) {
    program.parse();
}