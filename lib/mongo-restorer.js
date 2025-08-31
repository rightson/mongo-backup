const { MongoClient } = require('mongodb');
const fs = require('fs').promises;
const path = require('path');
const { createReadStream } = require('fs');
const { createGunzip } = require('zlib');
const readline = require('readline');
const { spawn } = require('child_process');

class MongoRestorer {
    constructor(options) {
        this.options = options;
        this.database = options.database;
        this.collection = options.collection;
        this.targetDatabase = options.targetDatabase || options.database;
        this.allCollections = options.allCollections || false;
        this.inputDir = options.inputDir || './dump-backup';
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
        this.db = this.client.db(this.targetDatabase);
        if (this.collection) {
            this.coll = this.db.collection(this.collection);
        }
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
                // Show real-time progress from mongorestore
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

    async discoverCollections() {
        console.log('Discovering collections in dump directory...');
        
        try {
            const items = await fs.readdir(this.inputDir);
            const collections = new Set();

            // Look for chunk directories (YYYY-MM format)
            for (const item of items) {
                const chunkPath = path.join(this.inputDir, item);
                const stat = await fs.stat(chunkPath).catch(() => null);
                
                if (stat && stat.isDirectory() && item.match(/^\d{4}-\d{2}$/)) {
                    // This is a monthly chunk directory
                    const dbPath = path.join(chunkPath, this.database);
                    const dbStat = await fs.stat(dbPath).catch(() => null);
                    
                    if (dbStat && dbStat.isDirectory()) {
                        // Look for collection BSON files
                        const dbFiles = await fs.readdir(dbPath);
                        dbFiles.forEach(file => {
                            if (file.endsWith('.bson') || file.endsWith('.bson.gz')) {
                                const collectionName = file.replace(/\.bson(\.gz)?$/, '');
                                collections.add(collectionName);
                            }
                        });
                    }
                }
            }

            const collectionList = Array.from(collections).sort();
            console.log(`✓ Found ${collectionList.length} collections: ${collectionList.join(', ')}`);
            
            return collectionList;
        } catch (error) {
            throw new Error(`Error discovering collections: ${error.message}`);
        }
    }

    async findDumpChunks() {
        console.log('Scanning for dump chunks...');
        
        try {
            const items = await fs.readdir(this.inputDir);
            const chunks = [];

            // Look for chunk directories (YYYY-MM format)
            for (const item of items) {
                const chunkPath = path.join(this.inputDir, item);
                const stat = await fs.stat(chunkPath).catch(() => null);
                
                if (stat && stat.isDirectory() && item.match(/^\d{4}-\d{2}$/)) {
                    // This is a monthly chunk directory
                    const dbPath = path.join(chunkPath, this.database);
                    const dbStat = await fs.stat(dbPath).catch(() => null);
                    
                    if (dbStat && dbStat.isDirectory()) {
                        // Look for collection BSON files
                        const dbFiles = await fs.readdir(dbPath);
                        const collectionFiles = dbFiles.filter(file => 
                            file.startsWith(this.collection + '.bson')
                        );

                        for (const file of collectionFiles) {
                            const filePath = path.join(dbPath, file);
                            chunks.push({
                                monthKey: item,
                                chunkDir: chunkPath,
                                filePath: filePath,
                                compressed: file.endsWith('.gz'),
                                sortKey: item
                            });
                        }
                    }
                }
            }

            chunks.sort((a, b) => a.sortKey.localeCompare(b.sortKey));
            
            console.log(`✓ Found ${chunks.length} dump chunks`);
            if (chunks.length > 0) {
                console.log(`  Date range: ${chunks[0].monthKey} to ${chunks[chunks.length - 1].monthKey}`);
            }

            return chunks;
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

    async restoreChunk(chunkInfo, chunkIndex, totalChunks) {
        const { monthKey, chunkDir } = chunkInfo;
        
        console.log(`[${chunkIndex + 1}/${totalChunks}] Restoring ${monthKey}...`);

        // Build mongorestore command arguments
        const args = [
            '--uri', this.uri,
            '--gzip', // Always assume gzip since mongodump creates .gz files
            chunkDir
        ];

        // Add namespace transformation if target database is different
        if (this.targetDatabase !== this.database) {
            args.push('--nsFrom', `${this.database}.*`);
            args.push('--nsTo', `${this.targetDatabase}.*`);
        }

        try {
            const result = await this.executeCommand('mongorestore', args);
            
            // Extract document count from mongorestore output if available
            let docCount = 0;
            if (result.stderr.includes('documents')) {
                const match = result.stderr.match(/(\d+)\s+documents?/);
                if (match) {
                    docCount = parseInt(match[1]);
                }
            }

            console.log(`  ✓ Completed: ${docCount.toLocaleString()} documents`);
            return { monthKey, documents: docCount };

        } catch (error) {
            throw error;
        }
    }

    async restoreAllChunks() {
        return await this.run();
    }

    async restoreSpecificChunks(monthKeys) {
        try {
            await this.connect();

            if (this.drop) {
                console.log(`Dropping collection ${this.database}.${this.collection}...`);
                await this.coll.drop().catch(() => {});
                console.log('✓ Collection dropped');
            }

            // Load indexes to restore (unless skipped)
            const indexes = this.skipIndexRestoration ? [] : await this.loadIndexes();

            const chunks = await this.findDumpChunks();
            const targetChunks = chunks.filter(chunk => monthKeys.includes(chunk.monthKey));

            if (targetChunks.length === 0) {
                console.log(`No chunks found for specified months: ${monthKeys.join(', ')}`);
                return;
            }

            console.log(`Found ${targetChunks.length} chunks to restore for months: ${monthKeys.join(', ')}`);

            const results = [];
            for (let i = 0; i < targetChunks.length; i++) {
                const chunkInfo = targetChunks[i];
                
                try {
                    const result = await this.restoreChunk(chunkInfo, i, targetChunks.length);
                    results.push(result);

                    // Show overall progress
                    const overallPercent = ((i + 1) / targetChunks.length * 100).toFixed(1);
                    console.log(`Overall Progress: ${i + 1}/${targetChunks.length} chunks completed (${overallPercent}%)`);

                } catch (error) {
                    console.error(`\n✗ Error processing ${chunkInfo.monthKey}:`, error.message);
                    throw error;
                }
            }

            // Restore indexes after all data is restored (unless skipped)
            if (!this.skipIndexRestoration) {
                await this.restoreIndexes(indexes);
            }

            console.log('\n\n=== SELECTIVE RESTORE COMPLETED ===');
            console.log(`Total chunks processed: ${results.length}`);
            console.log(`Total documents restored: ${results.reduce((sum, r) => sum + r.documents, 0).toLocaleString()}`);
            console.log(`Target collection: ${this.database}.${this.collection}`);

        } finally {
            await this.disconnect();
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
            console.log(`Resuming from state: ${state.restoredFiles.length} chunks completed`);

            const chunks = await this.findDumpChunks();

            if (chunks.length === 0) {
                console.log('No dump chunks found to restore');
                return;
            }

            const pendingChunks = chunks.filter(chunk => 
                !state.restoredFiles.includes(chunk.monthKey)
            );

            console.log(`Total chunks to process: ${chunks.length}`);
            console.log(`Pending chunks: ${pendingChunks.length}`);

            if (pendingChunks.length === 0) {
                console.log('✓ All chunks already restored!');
                // Still restore indexes if they haven't been restored yet (unless skipped)
                if (!this.skipIndexRestoration) {
                    await this.restoreIndexes(indexes);
                }
                return;
            }

            const results = [];
            for (let i = 0; i < pendingChunks.length; i++) {
                const chunkInfo = pendingChunks[i];
                
                try {
                    const result = await this.restoreChunk(chunkInfo, i, pendingChunks.length);
                    results.push(result);

                    // Update state after processing chunk
                    state.restoredFiles.push(chunkInfo.monthKey);
                    state.lastProcessed = new Date().toISOString();
                    await this.saveState(state);

                    // Show overall progress
                    const overallPercent = ((i + 1) / pendingChunks.length * 100).toFixed(1);
                    console.log(`Overall Progress: ${i + 1}/${pendingChunks.length} chunks completed (${overallPercent}%)`);

                } catch (error) {
                    console.error(`\n✗ Error processing ${chunkInfo.monthKey}:`, error.message);
                    throw error;
                }
            }

            // Restore indexes after all data is restored (unless skipped)
            if (!this.skipIndexRestoration) {
                await this.restoreIndexes(indexes);
            }

            console.log('\n\n=== RESTORE COMPLETED ===');
            console.log(`Total chunks processed: ${results.length}`);
            console.log(`Total documents restored: ${results.reduce((sum, r) => sum + r.documents, 0).toLocaleString()}`);
            console.log(`Target collection: ${this.database}.${this.collection}`);

            if (state.restoredFiles.length === chunks.length) {
                await fs.unlink(this.stateFile).catch(() => {});
                console.log('✓ Clean completion - state file removed');
            }

        } finally {
            await this.disconnect();
        }
    }

    async restoreAllCollections(months = null) {
        try {
            await this.connect();

            const collections = await this.discoverCollections();
            
            if (collections.length === 0) {
                console.log('No collections found in dump directory');
                return;
            }

            console.log(`\nRestoring ${collections.length} collections...`);
            
            const results = [];
            
            for (let collIndex = 0; collIndex < collections.length; collIndex++) {
                const collectionName = collections[collIndex];
                console.log(`\n[${collIndex + 1}/${collections.length}] === Restoring collection: ${collectionName} ===`);
                
                // Create a new restorer instance for this collection
                const collectionOptions = {
                    ...this.options,
                    collection: collectionName,
                    targetDatabase: this.targetDatabase
                };
                
                const collectionRestorer = new MongoRestorer(collectionOptions);
                
                try {
                    if (months) {
                        await collectionRestorer.restoreSpecificChunks(months);
                    } else {
                        await collectionRestorer.run();
                    }
                    
                    results.push({
                        collection: collectionName,
                        status: 'completed'
                    });
                    
                    console.log(`✓ Collection ${collectionName} restored successfully`);
                    
                } catch (error) {
                    console.error(`✗ Error restoring collection ${collectionName}:`, error.message);
                    results.push({
                        collection: collectionName,
                        status: 'failed',
                        error: error.message
                    });
                }
            }

            console.log('\n\n=== ALL COLLECTIONS RESTORE SUMMARY ===');
            console.log(`Total collections processed: ${results.length}`);
            
            const successful = results.filter(r => r.status === 'completed');
            const failed = results.filter(r => r.status === 'failed');
            
            console.log(`Successful: ${successful.length}`);
            if (successful.length > 0) {
                successful.forEach(r => console.log(`  ✓ ${r.collection}`));
            }
            
            if (failed.length > 0) {
                console.log(`Failed: ${failed.length}`);
                failed.forEach(r => console.log(`  ✗ ${r.collection}: ${r.error}`));
            }
            
            console.log(`Target database: ${this.targetDatabase}`);

        } finally {
            await this.disconnect();
        }
    }
}

module.exports = { MongoRestorer };