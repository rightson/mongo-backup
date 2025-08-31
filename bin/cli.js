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
    .option('-f, --date-field <field>', 'Date field for splitting', 'createdAt')
    .option('-o, --output-dir <dir>', 'Output directory', './dump-extra')
    .option('-b, --batch-size <size>', 'Batch size for querying', '50000')
    .option('-z, --compress', 'Compress output files with gzip (default: true)')
    .option('--no-compress', 'Disable compression')
    .option('--format <format>', 'Output format (json|bson)', 'json')
    .option('--skip-index-extraction', 'Skip automatic index extraction and saving (default: false)')
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
            skipIndexExtraction: options.skipIndexExtraction || false
        };

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

// Restore command
program
    .command('restore')
    .description('Restore MongoDB collection from monthly dump files')
    .option('-u, --uri <uri>', 'MongoDB connection URI', 'mongodb://localhost:27017')
    .option('-h, --host <host>', 'MongoDB host', 'localhost')
    .option('-p, --port <port>', 'MongoDB port', '27017')
    .option('--username <username>', 'MongoDB username')
    .option('--password <password>', 'MongoDB password')
    .option('--authentication-database <db>', 'Authentication database')
    .option('-d, --database <name>', 'Database name', 'test')
    .option('-c, --collection <name>', 'Collection name')
    .option('-i, --input-dir <dir>', 'Input directory containing dump files', './dump-extra')
    .option('-b, --batch-size <size>', 'Batch size for inserting', '25000')
    .option('--drop', 'Drop collection before restore')
    .option('--skip-index-restoration', 'Skip automatic index restoration (default: false)')
    .action(async (options) => {
        if (!options.collection) {
            console.error('Error: Collection name is required');
            process.exit(1);
        }

        // Parse numeric options
        const parsedOptions = {
            ...options,
            batchSize: parseInt(options.batchSize),
            port: parseInt(options.port),
            skipIndexRestoration: options.skipIndexRestoration || false
        };

        const restorer = new MongoRestorer(parsedOptions);

        try {
            await restorer.run();
            console.log('\n✓ Restore completed!');
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