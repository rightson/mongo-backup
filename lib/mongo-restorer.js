const { MongoClient } = require('mongodb');
const fs = require('fs').promises;
const path = require('path');
const { createReadStream } = require('fs');
const { createGunzip } = require('zlib');
const readline = require('readline');

class MongoRestorer {
    constructor(options) {
        this.options = options;
        this.database = options.database;
        this.collection = options.collection;
        this.inputDir = options.inputDir || './dump-extra';
        this.batchSize = options.batchSize || 25000;
        this.drop = options.drop || false;
        this.skipIndexRestoration = options.skipIndexRestoration || false;
        this.stateFile = path.join(this.inputDir, '.restore-state.json');
        this.client = null;
        this.db = null;
        this.coll = null;
        this.uri = null;
    }

    async promptPassword() {
        return new Promise((resolve) => {
            const rl = readline.createInterface({
                input: process.stdin,
                output: process.stdout
            });

            rl.stdoutMuted = true;
            rl.question('Enter password: ', (password) => {
                rl.stdoutMuted = false;
                rl.close();
                console.log();
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
        if (this.options.uri) {
            this.uri = this.options.uri;
            return;
        }

        let { host, port, username, password, authenticationDatabase } = this.options;
        
        host = host || 'localhost';
        port = port || 27017;
        authenticationDatabase = authenticationDatabase || this.database;

        if (username && !password) {
            password = await this.promptPassword();
        }

        let uri = 'mongodb://';
        
        if (username) {
            uri += encodeURIComponent(username);
            if (password) {
                uri += ':' + encodeURIComponent(password);
            }
            uri += '@';
        }

        uri += `${host}:${port}`;

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

    async loadState() {
        try {
            const stateData = await fs.readFile(this.stateFile, 'utf8');
            return JSON.parse(stateData);
        } catch (error) {
            return { restoredFiles: [], lastProcessed: null };
        }
    }

    async saveState(state) {
        await fs.writeFile(this.stateFile, JSON.stringify(state, null, 2));
    }

    async findDumpFiles() {
        console.log('Scanning for dump files...');
        
        try {
            const files = await fs.readdir(this.inputDir);
            const dumpFiles = files.filter(file => {
                const match = file.match(/^(.+)_(.+)_(\d{4}-\d{2})\.(jsonl?|bson)(\.gz)?$/);
                if (!match) return false;
                
                const [, dbName, collName] = match;
                return (!this.database || dbName === this.database) && 
                       (!this.collection || collName === this.collection);
            });

            const parsedFiles = dumpFiles.map(file => {
                const match = file.match(/^(.+)_(.+)_(\d{4}-\d{2})\.(jsonl?|bson)(\.gz)?$/);
                const [, dbName, collName, monthKey, format, compressed] = match;
                
                return {
                    filename: file,
                    path: path.join(this.inputDir, file),
                    database: dbName,
                    collection: collName,
                    monthKey: monthKey,
                    format: format,
                    compressed: !!compressed,
                    sortKey: monthKey
                };
            });

            parsedFiles.sort((a, b) => a.sortKey.localeCompare(b.sortKey));
            
            console.log(`✓ Found ${parsedFiles.length} dump files`);
            if (parsedFiles.length > 0) {
                console.log(`  Date range: ${parsedFiles[0].monthKey} to ${parsedFiles[parsedFiles.length - 1].monthKey}`);
            }

            return parsedFiles;
        } catch (error) {
            throw new Error(`Error reading input directory: ${error.message}`);
        }
    }

    async loadIndexes() {
        const indexFilename = `${this.database}_${this.collection}_indexes.json`;
        const indexFilePath = path.join(this.inputDir, indexFilename);
        
        try {
            console.log('Looking for saved indexes...');
            const indexData = await fs.readFile(indexFilePath, 'utf8');
            const parsed = JSON.parse(indexData);
            
            console.log(`✓ Found ${parsed.indexes.length} indexes to restore`);
            if (parsed.indexes.length > 0) {
                parsed.indexes.forEach(index => {
                    const keyStr = Object.keys(index.key).map(field => 
                        `${field}:${index.key[field]}`
                    ).join(', ');
                    console.log(`  - ${index.name}: {${keyStr}}`);
                });
            }
            
            return parsed.indexes;
            
        } catch (error) {
            if (error.code === 'ENOENT') {
                console.log('No index file found - skipping index restoration');
            } else {
                console.warn(`⚠ Warning: Failed to load indexes: ${error.message}`);
            }
            return [];
        }
    }

    async restoreIndexes(indexes) {
        if (indexes.length === 0) {
            console.log('No indexes to restore');
            return;
        }

        console.log(`\nRestoring ${indexes.length} indexes...`);
        
        for (const indexDef of indexes) {
            try {
                const { name, key, ...options } = indexDef;
                
                // Remove undefined/null values from options
                const cleanOptions = Object.keys(options).reduce((acc, optKey) => {
                    if (options[optKey] !== undefined && options[optKey] !== null) {
                        acc[optKey] = options[optKey];
                    }
                    return acc;
                }, {});

                console.log(`Creating index '${name}'...`);
                await this.coll.createIndex(key, { name, ...cleanOptions });
                console.log(`✓ Index '${name}' created successfully`);
                
            } catch (error) {
                if (error.code === 85) { // Index already exists
                    console.log(`✓ Index '${indexDef.name}' already exists`);
                } else {
                    console.warn(`⚠ Warning: Failed to create index '${indexDef.name}': ${error.message}`);
                }
            }
        }
        
        console.log('✓ Index restoration completed');
    }

    async restoreFile(fileInfo, progressCallback) {
        const { filename, path: filePath, monthKey, format, compressed } = fileInfo;
        
        console.log(`\nRestoring ${monthKey}...`);
        console.log(`  File: ${filename}`);

        let readStream = createReadStream(filePath);
        if (compressed) {
            const gunzipStream = createGunzip();
            readStream.pipe(gunzipStream);
            readStream = gunzipStream;
        }

        let documents = [];
        let totalProcessed = 0;
        let lineCount = 0;

        // Both JSONL and BSON formats use JSONL (one JSON object per line)
        // This is much more memory efficient for large collections
        if (format === 'jsonl' || format === 'json' || format === 'bson') {
            const rl = readline.createInterface({
                input: readStream,
                crlfDelay: Infinity
            });

            const batch = [];
            
            for await (const line of rl) {
                if (line.trim()) {
                    try {
                        const doc = JSON.parse(line.trim());
                        batch.push(doc);
                        lineCount++;

                        if (batch.length >= this.batchSize) {
                            try {
                                await this.coll.insertMany(batch, { ordered: false });
                                totalProcessed += batch.length;
                                progressCallback && progressCallback(totalProcessed, lineCount);
                            } catch (error) {
                                if (error.code === 11000) {
                                    console.log(`    Warning: Some documents already exist (duplicate key errors)`);
                                    totalProcessed += batch.length;
                                } else {
                                    throw error;
                                }
                            }
                            
                            batch.length = 0;
                            await new Promise(resolve => setImmediate(resolve));
                        }
                    } catch (parseError) {
                        console.warn(`  Warning: Skipping invalid JSON line: ${line.substring(0, 100)}...`);
                    }
                }
            }

            if (batch.length > 0) {
                try {
                    await this.coll.insertMany(batch, { ordered: false });
                    totalProcessed += batch.length;
                } catch (error) {
                    if (error.code === 11000) {
                        console.log(`    Warning: Some documents already exist (duplicate key errors)`);
                        totalProcessed += batch.length;
                    } else {
                        throw error;
                    }
                }
            }

            console.log(`  ✓ Completed: ${totalProcessed.toLocaleString()} documents`);
            return { monthKey, documents: totalProcessed };
        }
    }

    async run() {
        try {
            await this.connect();

            if (this.drop) {
                console.log(`Dropping collection ${this.database}.${this.collection}...`);
                await this.coll.drop().catch(() => {});
                console.log('✓ Collection dropped');
            }

            // Load indexes to restore (unless skipped)
            const indexes = this.skipIndexRestoration ? [] : await this.loadIndexes();

            const state = await this.loadState();
            console.log(`Resuming from state: ${state.restoredFiles.length} files completed`);

            const dumpFiles = await this.findDumpFiles();

            if (dumpFiles.length === 0) {
                console.log('No dump files found to restore');
                return;
            }

            const pendingFiles = dumpFiles.filter(file => 
                !state.restoredFiles.includes(file.filename)
            );

            console.log(`Pending files: ${pendingFiles.length}`);

            if (pendingFiles.length === 0) {
                console.log('✓ All files already restored!');
                // Still restore indexes if they haven't been restored yet (unless skipped)
                if (!this.skipIndexRestoration) {
                    await this.restoreIndexes(indexes);
                }
                return;
            }

            const results = [];
            for (let i = 0; i < pendingFiles.length; i++) {
                const fileInfo = pendingFiles[i];
                
                console.log(`\n[${i + 1}/${pendingFiles.length}] Processing ${fileInfo.filename}...`);
                
                try {
                    const result = await this.restoreFile(fileInfo, (processed, total) => {
                        const percent = total > 0 ? ((processed / total) * 100).toFixed(1) : '100.0';
                        process.stdout.write(`\r  Progress: ${processed.toLocaleString()}${total > 0 ? `/${total.toLocaleString()}` : ''} (${percent}%)`);
                    });

                    results.push(result);

                    state.restoredFiles.push(fileInfo.filename);
                    state.lastProcessed = new Date().toISOString();
                    await this.saveState(state);

                } catch (error) {
                    console.error(`\n✗ Error processing ${fileInfo.filename}:`, error.message);
                    throw error;
                }
            }

            // Restore indexes after all data is restored (unless skipped)
            if (!this.skipIndexRestoration) {
                await this.restoreIndexes(indexes);
            }

            console.log('\n\n=== RESTORE COMPLETED ===');
            console.log(`Total files processed: ${results.length}`);
            console.log(`Total documents restored: ${results.reduce((sum, r) => sum + r.documents, 0).toLocaleString()}`);
            console.log(`Target collection: ${this.database}.${this.collection}`);

            if (state.restoredFiles.length === dumpFiles.length) {
                await fs.unlink(this.stateFile).catch(() => {});
                console.log('✓ Clean completion - state file removed');
            }

        } finally {
            await this.disconnect();
        }
    }
}

module.exports = { MongoRestorer };