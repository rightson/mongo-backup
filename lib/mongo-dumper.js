const { MongoClient } = require('mongodb');
const fs = require('fs').promises;
const path = require('path');
const { createWriteStream } = require('fs');
const { createGzip } = require('zlib');
const readline = require('readline');
const os = require('os');

class MongoDumper {
    constructor(options) {
        this.options = options;
        this.database = options.database;
        this.collection = options.collection;
        this.dateField = options.dateField || 'createdAt';
        this.outputDir = options.outputDir || './dump-extra';
        this.batchSize = options.batchSize || 50000;
        this.compress = options.compress !== undefined ? options.compress : true;
        this.format = options.format || 'json'; // json or bson
        this.skipIndexExtraction = options.skipIndexExtraction || false;
        this.stateFile = path.join(this.outputDir, '.dump-state.json');
        this.client = null;
        this.db = null;
        this.coll = null;
        this.uri = null; // Will be built in buildConnectionUri()
        
        // Memory monitoring for large datasets
        this.initialMemory = process.memoryUsage();
        this.memoryThreshold = os.totalmem() * 0.8; // Use max 80% of system memory
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
        let current = new Date(minDate.getFullYear(), minDate.getMonth(), 1);
        const end = new Date(maxDate.getFullYear(), maxDate.getMonth() + 1, 1);

        while (current < end) {
            const monthStart = new Date(current);
            const monthEnd = new Date(current.getFullYear(), current.getMonth() + 1, 1);
            
            ranges.push({
                start: monthStart,
                end: monthEnd,
                key: `${monthStart.getFullYear()}-${String(monthStart.getMonth() + 1).padStart(2, '0')}`
            });

            current.setMonth(current.getMonth() + 1);
        }

        return ranges;
    }

    async generateAdaptiveRanges(monthRange, maxDocsPerRange = 1000000) {
        // For months with too many documents, split into smaller ranges
        const { start, end } = monthRange;
        const totalDocs = await this.getMonthlyDocumentCount(start, end);
        
        if (totalDocs <= maxDocsPerRange) {
            return [monthRange]; // Month is small enough
        }
        
        console.log(`    📊 Month ${monthRange.key} has ${totalDocs.toLocaleString()} documents, splitting into smaller ranges...`);
        
        // Calculate number of sub-ranges needed
        const numRanges = Math.ceil(totalDocs / maxDocsPerRange);
        const ranges = [];
        
        // Split month into equal time periods
        const monthDuration = end.getTime() - start.getTime();
        const subRangeDuration = Math.floor(monthDuration / numRanges);
        
        for (let i = 0; i < numRanges; i++) {
            const subStart = new Date(start.getTime() + (i * subRangeDuration));
            const subEnd = i === numRanges - 1 ? end : new Date(start.getTime() + ((i + 1) * subRangeDuration));
            
            ranges.push({
                start: subStart,
                end: subEnd,
                key: `${monthRange.key}-part${i + 1}`
            });
        }
        
        console.log(`    ✓ Split into ${ranges.length} sub-ranges of ~${Math.floor(maxDocsPerRange / 1000)}K documents each`);
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

    async dumpMonth(monthRange, progressCallback) {
        const { start, end, key } = monthRange;
        const filename = `${this.database}_${this.collection}_${key}`;
        const extension = this.format === 'bson' ? '.bson' : '.jsonl';
        const compressExt = this.compress ? '.gz' : '';
        const fullPath = path.join(this.outputDir, `${filename}${extension}${compressExt}`);

        console.log(`
Dumping ${key}...`);

        if (this.format !== 'json') {
            console.warn(`⚠ Warning: BSON format is not fully supported. Outputting in JSONL format.`);
        }

        // Create write stream with optimized buffer size
        let writeStream = createWriteStream(fullPath, { 
            highWaterMark: 64 * 1024 // 64KB buffer for better I/O performance
        });
        
        if (this.compress) {
            const gzipStream = createGzip({
                level: 6, // Balanced compression level (1=fastest, 9=best compression)
                chunkSize: 64 * 1024 // Match write stream buffer
            });
            gzipStream.pipe(writeStream);
            writeStream = gzipStream;
        }

        // Query with proper indexing hint and memory-efficient options
        const query = {
            [this.dateField]: {
                $gte: start,
                $lt: end
            }
        };

        // For documents with 4000+ key/value pairs, use ULTRA conservative approach
        // Each key/value pair can consume ~100-500 bytes in memory during JSON.stringify()
        const estimatedKeysPerDoc = 4000;
        const memoryOverheadPerKey = 500; // Conservative estimate including JSON serialization overhead
        const estimatedDocMemoryFootprint = estimatedKeysPerDoc * memoryOverheadPerKey; // ~2MB overhead per doc
        
        // ALWAYS process one document at a time for complex documents
        const effectiveBatchSize = 1;
        
        console.log(`  🔧 Processing documents individually for memory efficiency`);
        
        const cursor = this.coll.find(query)
            .batchSize(1) // MongoDB driver batch size = 1
            .sort({ [this.dateField]: 1 }); // Ensure consistent ordering

        let processedDocs = 0;
        let lastProgressTime = Date.now();
        let totalDocSize = 0;
        let largestDocSize = 0;
        let maxKeyCount = 0;

        try {
            // TRUE STREAMING: Manual cursor iteration - no internal buffering
            let doc;
            while ((doc = await cursor.next()) !== null) {
                const startMemory = process.memoryUsage().heapUsed;
                
                // Analyze document complexity
                const keyCount = this.countObjectKeys(doc);
                if (keyCount > maxKeyCount) {
                    maxKeyCount = keyCount;
                }
                
                // Use streaming approach for JSON serialization to minimize memory spike
                let docJson;
                try {
                    docJson = JSON.stringify(doc);
                } catch (error) {
                    // Handle potential JSON serialization errors for complex objects
                    console.log(`    ⚠️  JSON serialization error for document, using fallback...`);
                    docJson = this.safeStringify(doc);
                }
                
                const docSize = Buffer.byteLength(docJson, 'utf8');
                const memoryAfterStringify = process.memoryUsage().heapUsed;
                const serializationMemorySpike = memoryAfterStringify - startMemory;
                
                // Track document statistics
                totalDocSize += docSize;
                if (docSize > largestDocSize) {
                    largestDocSize = docSize;
                }
                
                // Write immediately to minimize memory footprint
                await this.writeToStreamSafely(writeStream, docJson + '\n');
                
                // Aggressive cleanup - nullify all references immediately
                docJson = null;
                doc = null;
                
                processedDocs++;
                
                // Ultra-frequent memory management for complex documents
                const now = Date.now();
                if (processedDocs % 10 === 0 || now - lastProgressTime > 2000) { // Every 10 docs or 2 seconds
                    const memoryUsage = this.getMemoryUsage();
                    const avgDocSize = totalDocSize / processedDocs;
                    const avgKeys = Math.floor(maxKeyCount / Math.max(1, processedDocs * 0.1)); // Rough estimate
                    
                    // Clean progress reporting
                    progressCallback && progressCallback(processedDocs);
                    
                    lastProgressTime = now;
                    
                    // ULTRA aggressive GC for complex documents - force GC after EVERY document if memory is high
                    if (global.gc) {
                        if (memoryUsage.percentOfSystem > 40 || serializationMemorySpike > 50 * 1024 * 1024) {
                            global.gc();
                        }
                        // Always GC every 50 documents regardless
                        if (processedDocs % 50 === 0) {
                            global.gc();
                        }
                    }
                    
                    // Emergency measures for extreme memory pressure
                    if (memoryUsage.percentOfSystem > 75) {
                        // Emergency memory cleanup without verbose logging
                        if (global.gc) {
                            global.gc();
                            await new Promise(resolve => setTimeout(resolve, 50));
                            global.gc();
                            await new Promise(resolve => setTimeout(resolve, 50));
                            global.gc();
                        }
                        await new Promise(resolve => setTimeout(resolve, 200));
                    }
                }
                
                // Micro-pause after each document to allow GC cycles
                if (keyCount > 3000) {
                    await new Promise(resolve => setImmediate(resolve));
                }
            }

            // Final summary
            const avgDocSize = processedDocs > 0 ? totalDocSize / processedDocs : 0;
            console.log(`  📊 Avg document size: ${this.formatBytes(avgDocSize)}`);

            // Final progress update
            progressCallback && progressCallback(processedDocs);

            // Close stream properly
            await new Promise((resolve, reject) => {
                writeStream.end((error) => {
                    if (error) reject(error);
                    else resolve();
                });
            });

            console.log(`  ✓ Completed: ${processedDocs.toLocaleString()} documents`);
            return { key, documents: processedDocs, file: fullPath };

        } catch (error) {
            writeStream.destroy();
            // Clean up partial file
            try {
                await fs.unlink(fullPath);
            } catch (unlinkError) {
                // Ignore cleanup errors
            }
            throw error;
        } finally {
            // Ensure cursor is closed
            try {
                await cursor.close();
            } catch (closeError) {
                // Ignore cursor close errors
            }
        }
    }

    async writeToStreamSafely(stream, data) {
        return new Promise((resolve, reject) => {
            const canWriteMore = stream.write(data);
            if (canWriteMore) {
                resolve();
            } else {
                // Wait for drain event if buffer is full
                stream.once('drain', resolve);
                stream.once('error', reject);
            }
        });
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

    getMemoryUsage() {
        const usage = process.memoryUsage();
        return {
            rss: usage.rss, // Resident Set Size
            heapUsed: usage.heapUsed,
            heapTotal: usage.heapTotal,
            external: usage.external,
            percentOfSystem: (usage.rss / os.totalmem()) * 100
        };
    }

    adaptiveBatchSize(currentBatchSize, memoryUsage) {
        const memoryPercentUsed = memoryUsage.percentOfSystem;
        
        // If using more than 60% of system memory, reduce batch size
        if (memoryPercentUsed > 60) {
            return Math.max(1000, Math.floor(currentBatchSize * 0.7));
        }
        // If using less than 30% of system memory, can increase batch size (up to original)
        else if (memoryPercentUsed < 30 && currentBatchSize < this.batchSize) {
            return Math.min(this.batchSize, Math.floor(currentBatchSize * 1.2));
        }
        
        return currentBatchSize;
    }

    shouldForceGC(processedDocs, memoryUsage) {
        // Force GC if memory usage is high or at regular intervals for large datasets
        return (memoryUsage.percentOfSystem > 70) || 
               (processedDocs > 0 && processedDocs % 50000 === 0);
    }

    countObjectKeys(obj, depth = 0, maxDepth = 10) {
        // Recursively count all keys in an object (including nested objects/arrays)
        // With depth limit to prevent infinite recursion
        if (depth > maxDepth || obj === null || typeof obj !== 'object') {
            return 0;
        }
        
        let count = 0;
        try {
            if (Array.isArray(obj)) {
                count = obj.length; // Count array elements
                for (const item of obj) {
                    count += this.countObjectKeys(item, depth + 1, maxDepth);
                }
            } else {
                const keys = Object.keys(obj);
                count = keys.length; // Count direct keys
                for (const key of keys) {
                    count += this.countObjectKeys(obj[key], depth + 1, maxDepth);
                }
            }
        } catch (error) {
            // Handle circular references or other issues
            return count;
        }
        
        return count;
    }

    safeStringify(obj) {
        // Safe JSON stringification with circular reference handling
        const seen = new WeakSet();
        try {
            return JSON.stringify(obj, (key, val) => {
                if (val !== null && typeof val === "object") {
                    if (seen.has(val)) {
                        return "[Circular Reference]";
                    }
                    seen.add(val);
                }
                return val;
            });
        } catch (error) {
            // Ultimate fallback - convert to string representation
            return `{"_error": "Failed to serialize document: ${error.message}", "_toString": "${obj.toString()}"}`;
        }
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

            // Asynchronously get the total document count for ETR calculation
            let totalDocs = null;
            let countError = null;
            this.coll.countDocuments({})
                .then(count => totalDocs = count)
                .catch(err => countError = err);

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

            // Process each month with adaptive splitting
            const results = [];
            let cumulativeDocs = 0;
            const startTime = Date.now();

            for (let i = 0; i < pendingRanges.length; i++) {
                const monthRange = pendingRanges[i];
                
                console.log(`
[${i + 1}/${pendingRanges.length}] Processing ${monthRange.key}...`);
                
                try {
                    // Check if month needs splitting for memory efficiency
                    const adaptiveRanges = await this.generateAdaptiveRanges(monthRange);
                    
                    // Process each sub-range (could be just 1 if month is small)
                    for (let j = 0; j < adaptiveRanges.length; j++) {
                        const range = adaptiveRanges[j];
                        
                        if (adaptiveRanges.length > 1) {
                            console.log(`  [${j + 1}/${adaptiveRanges.length}] Processing ${range.key}...`);
                        }
                        
                        const result = await this.dumpMonth(range, (processed) => {
                            const currentTotalProcessed = cumulativeDocs + processed;
                            const elapsedTime = (Date.now() - startTime) / 1000; // in seconds

                            if (totalDocs === null) {
                                // Still waiting for total count
                                process.stdout.write(`\r  Progress: ${currentTotalProcessed.toLocaleString()} documents`);
                            } else if (totalDocs > 0) {
                                // Total count is available, show completion percentage
                                const percent = ((currentTotalProcessed / totalDocs) * 100).toFixed(1);
                                process.stdout.write(`\r  Progress: ${currentTotalProcessed.toLocaleString()}/${totalDocs.toLocaleString()} (${percent}%)`);
                            }
                        });

                        cumulativeDocs += result.documents;
                        results.push(result);
                    }

                    // Update state after processing entire month (including all sub-ranges)
                    state.completedMonths.push(monthRange.key);
                    state.lastProcessed = new Date().toISOString();
                    await this.saveState(state);

                } catch (error) {
                    console.error(`
✗ Error processing ${monthRange.key}:`, error.message);
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