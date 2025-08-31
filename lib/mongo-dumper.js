const { MongoClient } = require('mongodb');
const fs = require('fs').promises;
const path = require('path');
const { createWriteStream } = require('fs');
const { createGzip } = require('zlib');
const readline = require('readline');

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

        // Create write stream
        let writeStream = createWriteStream(fullPath);
        if (this.compress) {
            const gzipStream = createGzip();
            gzipStream.pipe(writeStream);
            writeStream = gzipStream;
        }

        // Query with proper indexing hint
        const query = {
            [this.dateField]: {
                $gte: start,
                $lt: end
            }
        };

        const cursor = this.coll.find(query).batchSize(this.batchSize);

        let processedDocs = 0;
        let lastProgressTime = Date.now();

        try {
            // Use a cursor stream for better progress reporting
            const stream = cursor.stream();

            stream.on('data', (doc) => {
                if (this.format === 'json') {
                    writeStream.write(JSON.stringify(doc) + '\n');
                } else {
                    writeStream.write(JSON.stringify(doc) + '\n');
                }

                processedDocs++;

                const now = Date.now();
                if (processedDocs % 1000 === 0 || now - lastProgressTime > 250) {
                    progressCallback && progressCallback(processedDocs);
                    lastProgressTime = now;
                }
            });

            await new Promise((resolve, reject) => {
                stream.on('end', resolve);
                stream.on('error', reject);
            });

            // Final progress update
            progressCallback && progressCallback(processedDocs);

            // Close stream
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

            // Process each month
            const results = [];
            let cumulativeDocs = 0;
            const startTime = Date.now();

            for (let i = 0; i < pendingRanges.length; i++) {
                const range = pendingRanges[i];
                
                console.log(`
[${i + 1}/${pendingRanges.length}] Processing ${range.key}...`);
                
                try {
                    const result = await this.dumpMonth(range, (processed) => {
                        const currentTotalProcessed = cumulativeDocs + processed;
                        const elapsedTime = (Date.now() - startTime) / 1000; // in seconds

                        if (totalDocs === null) {
                            // Still waiting for total count
                            process.stdout.write(`\r  Progress: ${currentTotalProcessed.toLocaleString()} documents | ETR: estimating...`);
                        } else if (elapsedTime > 0 && totalDocs > 0) {
                            // Total count is available, calculate ETR
                            const docsPerSecond = currentTotalProcessed / elapsedTime;
                            const remainingDocs = totalDocs - currentTotalProcessed;
                            
                            if (docsPerSecond > 0) {
                                const estimatedTimeRemaining = remainingDocs / docsPerSecond; // in seconds
                                
                                if (estimatedTimeRemaining > 0) {
                                    const formattedETR = new Date(estimatedTimeRemaining * 1000).toISOString().substr(11, 8);
                                    const percent = ((currentTotalProcessed / totalDocs) * 100).toFixed(1);
                                    process.stdout.write(`\r  Progress: ${currentTotalProcessed.toLocaleString()}/${totalDocs.toLocaleString()} (${percent}%) | ETR: ${formattedETR}`);
                                } else {
                                    // Almost done, or calculation is off
                                    process.stdout.write(`\r  Progress: ${currentTotalProcessed.toLocaleString()}/${totalDocs.toLocaleString()} (100.0%) | ETR: 00:00:00`);
                                }
                            }
                        }
                    });

                    cumulativeDocs += result.documents;
                    results.push(result);

                    // Update state
                    state.completedMonths.push(range.key);
                    state.lastProcessed = new Date().toISOString();
                    await this.saveState(state);

                } catch (error) {
                    console.error(`
✗ Error processing ${range.key}:`, error.message);
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