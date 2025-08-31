const { MongoClient } = require('mongodb');
const fs = require('fs').promises;
const path = require('path');
const readline = require('readline');
const { spawn } = require('child_process');

class MongoDumper {
    constructor(options) {
        this.options = options;
        this.database = options.database;
        this.collection = options.collection;
        this.dateField = options.dateField;
        this.outputDir = options.outputDir || './dump-backup';
        this.compress = options.compress !== undefined ? options.compress : true;
        this.skipIndexExtraction = options.skipIndexExtraction || false;
        this.debugListeners = options.debugListeners || false;
        this.stateFile = path.join(this.outputDir, '.dump-state.json');
        this.client = null;
        this.db = null;
        this.coll = null;
        this.uri = null; // Will be built in buildConnectionUri()
    }

    async promptPassword() {
        return new Promise((resolve) => {
            const rl = readline.createInterface({
                input: process.stdin,
                output: process.stdout
            });

            // Hide password input
            rl.stdoutMuted = true;
            rl.question('Enter password: ', (password) => {
                rl.stdoutMuted = false;
                rl.close();
                console.log(); // Add newline after hidden input
                resolve(password);
            });

            rl._writeToOutput = function _writeToOutput(stringToWrite) {
                if (rl.stdoutMuted)
                    rl.output.write('*');
                else
                    rl.output.write(stringToWrite);
            };
        });
    }

    async buildConnectionUri() {
        // If URI is provided directly, use it
        if (this.options.uri) {
            this.uri = this.options.uri;
            return;
        }

        // Build URI from individual components
        let { host, port, username, password, authenticationDatabase } = this.options;
        
        // Default values
        host = host || 'localhost';
        port = port || 27017;
        authenticationDatabase = authenticationDatabase || this.database;

        // Handle password prompting
        if (username && !password) {
            password = await this.promptPassword();
        }

        // Build the URI
        let uri = 'mongodb://';
        
        if (username) {
            uri += encodeURIComponent(username);
            if (password) {
                uri += ':' + encodeURIComponent(password);
            }
            uri += '@';
        }

        uri += `${host}:${port}`;

        // Add database and auth options
        if (username && authenticationDatabase && authenticationDatabase !== this.database) {
            uri += `/${authenticationDatabase}`;
        }

        this.uri = uri;
        console.log(`Using connection: mongodb://${username ? username + '@' : ''}${host}:${port}`);
    }

    async connect() {
        await this.buildConnectionUri();
        console.log('Connecting to MongoDB...');
        this.client = new MongoClient(this.uri);
        await this.client.connect();
        this.db = this.client.db(this.database);
        this.coll = this.db.collection(this.collection);
        console.log('✓ Connected to MongoDB');
    }

    async disconnect() {
        if (this.client) {
            await this.client.close();
            console.log('✓ Disconnected from MongoDB');
        }
    }

    async ensureOutputDir() {
        try {
            await fs.mkdir(this.outputDir, { recursive: true });
        } catch (error) {
            if (error.code !== 'EEXIST') throw error;
        }
    }

    async loadState() {
        try {
            const stateData = await fs.readFile(this.stateFile, 'utf8');
            return JSON.parse(stateData);
        } catch (error) {
            return { completedMonths: [], lastProcessed: null };
        }
    }

    async saveState(state) {
        await fs.writeFile(this.stateFile, JSON.stringify(state, null, 2));
    }

    async checkIndexExists(fieldName) {
        const indexes = await this.coll.indexes();
        return indexes.some(index => 
            index.key && (index.key[fieldName] === 1 || index.key[fieldName] === -1)
        );
    }


    async extractIndexes() {
        console.log('Extracting collection indexes...');
        
        try {
            const indexes = await this.coll.indexes();
            
            // Filter out the default _id index and system indexes
            const customIndexes = indexes.filter(index => 
                index.name !== '_id_' && !index.name.startsWith('$')
            );

            // Clean up index data for storage
            const indexData = customIndexes.map(index => ({
                name: index.name,
                key: index.key,
                unique: index.unique || false,
                sparse: index.sparse || false,
                background: index.background || false,
                partialFilterExpression: index.partialFilterExpression,
                expireAfterSeconds: index.expireAfterSeconds,
                weights: index.weights,
                textIndexVersion: index.textIndexVersion,
                '2dsphereIndexVersion': index['2dsphereIndexVersion'],
                bucketSize: index.bucketSize,
                min: index.min,
                max: index.max,
                // Include any other index options
                ...Object.keys(index).reduce((acc, key) => {
                    if (!['name', 'key', 'ns', 'v'].includes(key)) {
                        acc[key] = index[key];
                    }
                    return acc;
                }, {})
            }));

            console.log(`✓ Found ${customIndexes.length} custom indexes to preserve`);
            
            if (customIndexes.length > 0) {
                customIndexes.forEach(index => {
                    const keyStr = Object.keys(index.key).map(field => 
                        `${field}:${index.key[field]}`
                    ).join(', ');
                    console.log(`  - ${index.name}: {${keyStr}}`);
                });
            }

            return indexData;
            
        } catch (error) {
            console.warn(`⚠ Warning: Failed to extract indexes: ${error.message}`);
            return [];
        }
    }

    async saveIndexes(indexes) {
        if (indexes.length === 0) {
            console.log('No custom indexes to save');
            return;
        }

        const indexFilename = `${this.database}_${this.collection}_indexes.json`;
        const indexFilePath = path.join(this.outputDir, indexFilename);
        
        try {
            const indexData = {
                database: this.database,
                collection: this.collection,
                extractedAt: new Date().toISOString(),
                indexes: indexes
            };

            await fs.writeFile(indexFilePath, JSON.stringify(indexData, null, 2));
            console.log(`✓ Indexes saved to ${indexFilename}`);
            
        } catch (error) {
            console.warn(`⚠ Warning: Failed to save indexes: ${error.message}`);
        }
    }

    async listCollections() {
        await this.connect();
        
        try {
            const collections = await this.db.listCollections().toArray();
            const results = [];
            
            for (const collInfo of collections) {
                const collName = collInfo.name;
                try {
                    const count = await this.db.collection(collName).countDocuments();
                    results.push({ name: collName, count });
                } catch (error) {
                    results.push({ name: collName, count: 'Error' });
                }
            }
            
            return results;
        } finally {
            await this.disconnect();
        }
    }

    async getDateRange() {
        console.log('Analyzing date range...');
        
        // Get min and max dates
        const [minResult, maxResult] = await Promise.all([
            this.coll.find({}).sort({ [this.dateField]: 1 }).limit(1).toArray(),
            this.coll.find({}).sort({ [this.dateField]: -1 }).limit(1).toArray()
        ]);

        if (minResult.length === 0 || maxResult.length === 0) {
            throw new Error('Collection is empty or date field not found');
        }

        const minDate = new Date(minResult[0][this.dateField]);
        const maxDate = new Date(maxResult[0][this.dateField]);

        console.log(`✓ Date range: ${minDate.toISOString()} to ${maxDate.toISOString()}`);
        return { minDate, maxDate };
    }

    generateMonthlyRanges(minDate, maxDate) {
        const ranges = [];
        let current = new Date(Date.UTC(minDate.getFullYear(), minDate.getMonth(), 1));
        const end = new Date(Date.UTC(maxDate.getFullYear(), maxDate.getMonth() + 1, 1));

        while (current < end) {
            const monthStart = new Date(current);
            const monthEnd = new Date(Date.UTC(current.getUTCFullYear(), current.getUTCMonth() + 1, 1));
            
            ranges.push({
                start: monthStart,
                end: monthEnd,
                key: `${monthStart.getUTCFullYear()}-${String(monthStart.getUTCMonth() + 1).padStart(2, '0')}`
            });

            current.setUTCMonth(current.getUTCMonth() + 1);
        }

        return ranges;
    }


    async getMonthlyDocumentCount(start, end) {
        return await this.coll.countDocuments({
            [this.dateField]: {
                $gte: start,
                $lt: end
            }
        });
    }

    async executeCommand(command, args = []) {
        return new Promise((resolve, reject) => {
            console.log(`  Executing: ${command} ${args.join(' ')}`);
            
            const child = spawn(command, args, {
                stdio: ['pipe', 'pipe', 'pipe']
            });

            let stdout = '';
            let stderr = '';

            child.stdout.on('data', (data) => {
                const output = data.toString();
                stdout += output;
                // Show real-time progress from mongodump
                if (output.includes('documents')) {
                    process.stdout.write(`\r  ${output.trim()}`);
                }
            });

            child.stderr.on('data', (data) => {
                stderr += data.toString();
            });

            child.on('close', (code) => {
                if (code === 0) {
                    console.log(''); // New line after progress
                    resolve({ stdout, stderr, exitCode: code });
                } else {
                    reject(new Error(`Command failed with exit code ${code}: ${stderr}`));
                }
            });

            child.on('error', (error) => {
                reject(new Error(`Failed to execute command: ${error.message}`));
            });
        });
    }

    async dumpMonth(monthRange, monthIndex, totalMonths) {
        const { start, end, key } = monthRange;
        
        console.log(`[${monthIndex + 1}/${totalMonths}] Dumping ${key}...`);

        // Build mongodump query
        const query = `{"${this.dateField}":{"$gte":{"$date":"${start.toISOString()}"},"$lt":{"$date":"${end.toISOString()}"}}}`;
        
        // Create output directory for this chunk
        const chunkOutputDir = path.join(this.outputDir, key);
        await fs.mkdir(chunkOutputDir, { recursive: true });

        // Build mongodump command arguments
        const args = [
            '--uri', this.uri,
            '--db', this.database,
            '--collection', this.collection,
            '--query', query,
            '--out', chunkOutputDir
        ];

        // Add compression if enabled
        if (this.compress) {
            args.push('--gzip');
        }

        try {
            const result = await this.executeCommand('mongodump', args);
            
            // Count documents from the output
            const dumpFilePath = path.join(chunkOutputDir, this.database, `${this.collection}.bson${this.compress ? '.gz' : ''}`);
            let docCount = 0;
            
            // Extract document count from mongodump output if available
            if (result.stderr.includes('documents')) {
                const match = result.stderr.match(/(\d+)\s+documents?/);
                if (match) {
                    docCount = parseInt(match[1]);
                }
            }

            console.log(`  ✓ Completed: ${docCount.toLocaleString()} documents`);
            return { key, documents: docCount, file: dumpFilePath };

        } catch (error) {
            // Clean up partial directory on failure
            try {
                await fs.rmdir(chunkOutputDir, { recursive: true });
            } catch (cleanupError) {
                // Ignore cleanup errors
            }
            throw error;
        }
    }

    async validateBackupFile(monthKey) {
        const filename = `${this.database}_${this.collection}_${monthKey}`;
        const extension = this.format === 'bson' ? '.bson' : '.jsonl';
        const compressExt = this.compress ? '.gz' : '';
        const filePath = path.join(this.outputDir, `${filename}${extension}${compressExt}`);
        
        try {
            const stats = await fs.stat(filePath);
            if (stats.size === 0) {
                return { exists: false, valid: false, path: filePath, reason: 'File is empty' };
            }
            return { exists: true, valid: true, path: filePath, size: stats.size };
        } catch (error) {
            return { exists: false, valid: false, path: filePath, reason: error.message };
        }
    }

    async findBackupFiles() {
        const files = [];
        try {
            const dirContents = await fs.readdir(this.outputDir);
            const pattern = new RegExp(`^${this.database}_${this.collection}_(\\d{4}-\\d{2})\\.(jsonl|bson)(\\.gz)?$`);
            
            for (const file of dirContents) {
                const match = file.match(pattern);
                if (match) {
                    const monthKey = match[1];
                    const filePath = path.join(this.outputDir, file);
                    const stats = await fs.stat(filePath);
                    
                    files.push({
                        monthKey,
                        filename: file,
                        path: filePath,
                        size: stats.size,
                        modified: stats.mtime
                    });
                }
            }
            
            return files.sort((a, b) => a.monthKey.localeCompare(b.monthKey));
        } catch (error) {
            throw new Error(`Failed to scan backup directory: ${error.message}`);
        }
    }

    async cleanBackedUpData(options = {}) {
        const { months, confirmDelete = true, dryRun = false } = options;
        
        console.log('🔍 Scanning for backup files...');
        await this.ensureOutputDir();
        
        // Load state to verify which months are actually completed
        const state = await this.loadState();
        const completedMonths = new Set(state.completedMonths);
        
        // Find all backup files
        const backupFiles = await this.findBackupFiles();
        
        if (backupFiles.length === 0) {
            console.log('ℹ️  No backup files found');
            return { deleted: [], errors: [] };
        }
        
        // Filter files based on criteria
        let filesToDelete = backupFiles;
        
        // If specific months provided, filter to only those
        if (months && months.length > 0) {
            const monthSet = new Set(months);
            filesToDelete = backupFiles.filter(file => monthSet.has(file.monthKey));
        }
        
        // Only include files that are marked as completed in state
        filesToDelete = filesToDelete.filter(file => {
            if (!completedMonths.has(file.monthKey)) {
                console.log(`⚠️  Skipping ${file.monthKey}: not marked as completed in state file`);
                return false;
            }
            return true;
        });
        
        if (filesToDelete.length === 0) {
            console.log('ℹ️  No eligible files to delete');
            return { deleted: [], errors: [] };
        }
        
        // Validate each file before deletion
        console.log('🔍 Validating backup files...');
        const validationResults = await Promise.all(
            filesToDelete.map(file => this.validateBackupFile(file.monthKey))
        );
        
        const validFiles = [];
        const invalidFiles = [];
        
        filesToDelete.forEach((file, index) => {
            const validation = validationResults[index];
            if (validation.valid) {
                validFiles.push({ ...file, validation });
            } else {
                invalidFiles.push({ ...file, validation });
                console.log(`⚠️  Invalid backup: ${file.monthKey} - ${validation.reason}`);
            }
        });
        
        if (invalidFiles.length > 0) {
            console.log(`\n⚠️  Found ${invalidFiles.length} invalid backup files. These will be skipped.`);
        }
        
        if (validFiles.length === 0) {
            console.log('❌ No valid backup files to delete');
            return { deleted: [], errors: invalidFiles.map(f => f.validation.reason) };
        }
        
        // Show summary
        console.log(`\n📋 Summary:`);
        console.log(`   Valid backups found: ${validFiles.length}`);
        console.log(`   Total size: ${this.formatBytes(validFiles.reduce((sum, f) => sum + f.size, 0))}`);
        
        if (dryRun) {
            console.log('\n🔍 DRY RUN - Files that would be deleted:');
            validFiles.forEach(file => {
                console.log(`   • ${file.filename} (${this.formatBytes(file.size)})`);
            });
            return { deleted: [], errors: [], dryRun: validFiles };
        }
        
        // Confirmation prompt
        if (confirmDelete) {
            console.log('\n⚠️  The following backup files will be PERMANENTLY deleted:');
            validFiles.forEach(file => {
                console.log(`   • ${file.filename} (${this.formatBytes(file.size)})`);
            });
            
            const confirmed = await this.promptConfirmation('\n❓ Are you sure you want to delete these files? (y/N): ');
            if (!confirmed) {
                console.log('❌ Deletion cancelled');
                return { deleted: [], errors: [], cancelled: true };
            }
        }
        
        // Perform deletion
        console.log('\n🗑️  Deleting backup files...');
        const deleted = [];
        const errors = [];
        
        for (const file of validFiles) {
            try {
                await fs.unlink(file.path);
                console.log(`   ✓ Deleted: ${file.filename}`);
                deleted.push(file);
                
                // Update state to remove from completed months
                const monthIndex = state.completedMonths.indexOf(file.monthKey);
                if (monthIndex > -1) {
                    state.completedMonths.splice(monthIndex, 1);
                }
                
            } catch (error) {
                console.log(`   ✗ Failed to delete ${file.filename}: ${error.message}`);
                errors.push({ file: file.filename, error: error.message });
            }
        }
        
        // Save updated state
        if (deleted.length > 0) {
            await this.saveState(state);
        }
        
        // Also clean up index file if all months deleted
        if (state.completedMonths.length === 0) {
            const indexFilename = `${this.database}_${this.collection}_indexes.json`;
            const indexFilePath = path.join(this.outputDir, indexFilename);
            try {
                await fs.unlink(indexFilePath);
                console.log(`   ✓ Cleaned up index file: ${indexFilename}`);
            } catch (error) {
                // Index file might not exist, ignore
            }
            
            // Clean up empty state file
            try {
                await fs.unlink(this.stateFile);
                console.log('   ✓ Cleaned up state file');
            } catch (error) {
                // State file might not exist, ignore
            }
        }
        
        console.log(`\n✅ Cleanup completed: ${deleted.length} files deleted, ${errors.length} errors`);
        
        return { deleted, errors };
    }

    async promptConfirmation(message) {
        return new Promise((resolve) => {
            const rl = readline.createInterface({
                input: process.stdin,
                output: process.stdout
            });

            rl.question(message, (answer) => {
                rl.close();
                resolve(answer.toLowerCase().trim() === 'y');
            });
        });
    }

    formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    async dumpAllCollections() {
        console.log('Starting dump of all non-empty collections...');
        
        const collections = await this.listCollections();
        const nonEmptyCollections = collections.filter(coll => 
            typeof coll.count === 'number' && coll.count > 0
        );
        
        if (nonEmptyCollections.length === 0) {
            console.log('No non-empty collections found.');
            return;
        }
        
        console.log(`Found ${nonEmptyCollections.length} non-empty collections:`);
        nonEmptyCollections.forEach(coll => {
            console.log(`  • ${coll.name}: ${coll.count.toLocaleString()} documents`);
        });
        
        console.log('\nChecking date field availability...');
        
        // Check which collections have the specified date field
        const validCollections = [];
        for (const coll of nonEmptyCollections) {
            this.collection = coll.name; // Set current collection
            
            try {
                await this.connect();
                this.coll = this.db.collection(this.collection);
                
                // Check if date field exists in at least one document
                const sample = await this.coll.findOne({ [this.dateField]: { $exists: true } });
                if (sample) {
                    validCollections.push(coll);
                    console.log(`  ✓ ${coll.name}: has '${this.dateField}' field`);
                } else {
                    console.log(`  ✗ ${coll.name}: missing '${this.dateField}' field, skipping`);
                }
                
                await this.disconnect();
            } catch (error) {
                console.log(`  ✗ ${coll.name}: error checking field - ${error.message}`);
                await this.disconnect();
            }
        }
        
        if (validCollections.length === 0) {
            throw new Error(`No collections found with date field '${this.dateField}'`);
        }
        
        console.log(`\nDumping ${validCollections.length} collections with '${this.dateField}' field...`);
        
        // Dump each valid collection
        for (let i = 0; i < validCollections.length; i++) {
            const coll = validCollections[i];
            this.collection = coll.name;
            
            console.log(`\n[${i + 1}/${validCollections.length}] Processing collection: ${this.collection}`);
            console.log('─'.repeat(50));
            
            try {
                await this.run();
                console.log(`✓ Collection ${this.collection} completed`);
            } catch (error) {
                console.error(`✗ Collection ${this.collection} failed: ${error.message}`);
                // Continue with next collection instead of failing completely
            }
        }
        
        console.log('\n✓ All collections processing completed!');
    }

    async run() {
        try {
            await this.ensureOutputDir();
            await this.connect();

            // FIRST: Extract and save all indexes before any dump operations
            // This ensures indexes are preserved even if dump is interrupted early
            if (!this.skipIndexExtraction) {
                const indexes = await this.extractIndexes();
                await this.saveIndexes(indexes);
            }

            // Check if index exists on date field and warn if not (read-only check)
            const hasIndex = await this.checkIndexExists(this.dateField);
            if (!hasIndex) {
                console.log(`⚠ Warning: No index found on '${this.dateField}'. Query performance may be slow.`);
                console.log(`   Consider manually creating: db.${this.collection}.createIndex({"${this.dateField}": 1})`);
            }


            // Load previous state
            const state = await this.loadState();
            console.log(`Resuming from state: ${state.completedMonths.length} months completed`);

            // Get date range and generate monthly ranges
            const { minDate, maxDate } = await this.getDateRange();
            const monthlyRanges = this.generateMonthlyRanges(minDate, maxDate);
            
            console.log(`\nTotal months to process: ${monthlyRanges.length}`);

            // Filter out already completed months
            const pendingRanges = monthlyRanges.filter(range => 
                !state.completedMonths.includes(range.key)
            );

            console.log(`Pending months: ${pendingRanges.length}`);

            if (pendingRanges.length === 0) {
                console.log('✓ All months already completed!');
                return;
            }

            // Process each month
            const results = [];
            let cumulativeDocs = 0;

            for (let i = 0; i < pendingRanges.length; i++) {
                const monthRange = pendingRanges[i];
                
                try {
                    const result = await this.dumpMonth(monthRange, i, pendingRanges.length);
                    cumulativeDocs += result.documents;
                    results.push(result);

                    // Update state after processing month
                    state.completedMonths.push(monthRange.key);
                    state.lastProcessed = new Date().toISOString();
                    await this.saveState(state);

                    // Show overall progress
                    const overallPercent = ((i + 1) / pendingRanges.length * 100).toFixed(1);
                    console.log(`Overall Progress: ${i + 1}/${pendingRanges.length} months completed (${overallPercent}%)`);

                } catch (error) {
                    console.error(`\n✗ Error processing ${monthRange.key}:`, error.message);
                    throw error;
                }
            }

            // Summary
            console.log('\n\n=== DUMP COMPLETED ===');
            console.log(`Total months processed: ${results.length}`);
            console.log(`Total documents dumped: ${results.reduce((sum, r) => sum + r.documents, 0).toLocaleString()}`);
            console.log(`Output directory: ${this.outputDir}`);

            // Clean up state file on successful completion
            if (state.completedMonths.length === monthlyRanges.length) {
                await fs.unlink(this.stateFile).catch(() => {});
                console.log('✓ Clean completion - state file removed');
            }

        } finally {
            await this.disconnect();
        }
    }
}

module.exports = { MongoDumper };