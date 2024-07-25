import { TimestreamWriteClient } from "@aws-sdk/client-timestream-write";
import { TimestreamQueryClient } from "@aws-sdk/client-timestream-query";
import { TimestreamDependencyHelper } from "./utils/timestream-dependency-helper.js";
import * as resources from "./resources.js";
import * as queryExample from "./query.js";
import * as csvIngest from "./insert-csv.js";
import https from 'https';
import minimist from 'minimist';
import { Unload } from "./unload.js";
import { constants } from "./constants.js";
import { config } from "dotenv";

config()

const appType = {
    Basic: "basic",
    Unload: "unload",
    Cleanup: "cleanup",
    CompositePartitionKey: "compositePartitionKey",
}

const argv = minimist(process.argv.slice(2), {
    boolean: "skipDeletion"
});

const type = argv.type ?? appType.Basic;
const region =  "eu-west-1";
const skipDeletion = argv.skipDeletion ?? true;
const csvFilePath = argv.csvFilePath ?? null;

/**
 * Recommended Timestream write client SDK configuration:
 *  - Set SDK retry count to 10.
 *  - Use SDK DEFAULT_BACKOFF_STRATEGY
 *  - Set RequestTimeout to 20 seconds .
 *  - Set max connections to 5000 or higher.
 */
const agent = new https.Agent({
    maxSockets: 5000
});
const writeClient = new TimestreamWriteClient({
    endpoint: "https://ingest-cell1.timestream.eu-west-1.amazonaws.com",
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    },
    maxRetries: 10,
    httpOptions: {
        timeout: 20000,
        agent: agent
    },
    region: region
});
const queryClient = new TimestreamQueryClient({
    region: region
});

async function createResources() {
    await resources.createDatabase(writeClient);
    // await crudAndSimpleIngestionExample.describeDatabase(writeClient);
    // await crudAndSimpleIngestionExample.updateDatabase(argv.kmsKeyId, writeClient);
    await resources.listDatabases(writeClient);
    await resources.createTable(writeClient);
    // await crudAndSimpleIngestionExample.describeTable(writeClient);
    // await crudAndSimpleIngestionExample.updateTable(writeClient);
    await resources.listTables(writeClient);
}

async function callServices() {
    await createResources();

    if (csvFilePath != null) {
        await csvIngest.processCSV(writeClient, csvFilePath);
    }
    await queryExample.runAllQueries(queryClient);

    //Try cancelling a query
    //This could fail if there is no data in the table, and the example query has finished before it was cancelled.
    // await queryExample.tryCancelQuery(queryClient);

    // Try a query with multiple pages
    // await queryExample.tryQueryWithMultiplePages(queryClient, 20000);
}

async function callUnload() {
    const timestreamDependencyHelper = new TimestreamDependencyHelper(region);
    const account = await timestreamDependencyHelper.getAccount();
    const bucketName = constants.S3_BUCKET_PREFIX_UNLOAD + region + "-" + account;
    const unloadExample = new Unload(writeClient, queryClient, timestreamDependencyHelper, csvFilePath, bucketName);
    
    await createResources();
    await unloadExample.run();

    if (!skipDeletion) {
        await timestreamDependencyHelper.deleteS3Bucket(bucketName);
        await cleanup();
    }
}

async function cleanup() {
    await resources.deleteTable(writeClient, constants.DATABASE_NAME, constants.TABLE_NAME);
    await resources.deleteDatabase(writeClient);
}

switch (type) {
    case appType.Basic:
        callServices();
        break;
    case appType.Unload:
        callUnload();
        break;
    case appType.Cleanup:
        cleanup();
        break;
}
