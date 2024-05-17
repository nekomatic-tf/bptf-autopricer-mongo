// Stripped down from the main code, designed only to run as a snapshot worker
const fs = require('fs');
const chokidar = require('chokidar');

const methods = require('./methods');
const Methods = new methods();

const Schema = require('@tf2autobot/tf2-schema');

const config = require('./config.json');

const SCHEMA_PATH = './schema.json';
const ITEM_LIST_PATH = './files/item_list.json';

// Steam API key is required for the schema manager to work.
const schemaManager = new Schema({
    apiKey: config.steamAPIKey
});

// Listing descriptions that we want to ignore.
const excludedListingDescriptions = config.excludedListingDescriptions;

// Blocked attributes that we want to ignore. (Paints, parts, etc.)
const blockedAttributes = config.blockedAttributes;

const alwaysQuerySnapshotAPI = config.alwaysQuerySnapshotAPI;

// Create database instance for pg-promise.
const pgp = require('pg-promise')({
    schema: config.database.schema
});

// Create a database instance
const cn = {
    host: config.database.host,
    port: config.database.port,
    database: config.database.name,
    user: config.database.user,
    password: config.database.password
};

const db = pgp(cn);

// ColumnSet object for insert queries.
const cs = new pgp.helpers.ColumnSet(['name', 'sku', 'currencies', 'intent', 'updated', 'steamid'], {
    table: 'listings'
});

var stats = { // Stats for amount of items priced by what source.
    custom: 0,
    pricestf: 1 // This is always 1 due to the Mann Co. Supply Crate Key being classed under prices.tf
};

if (fs.existsSync(SCHEMA_PATH)) {
    // A cached schema exists.

    // Read and parse the cached schema.
    const cachedData = JSON.parse(fs.readFileSync(SCHEMA_PATH), 'utf8');

    // Set the schema data.
    schemaManager.setSchema(cachedData);
}

let allowedItemNames = new Set();

// Read and initialize the names of the items we want to get prices for.
const loadNames = () => {
    try {
        const jsonContent = JSON.parse(fs.readFileSync(ITEM_LIST_PATH), 'utf8');
        if (jsonContent && jsonContent.items && Array.isArray(jsonContent.items)) {
            allowedItemNames = new Set(jsonContent.items.map(item => item.name));
            console.log('Updated allowed item names.');
        }
    } catch (error) {
        console.error('Error reading and updating allowed item names:', error);
    }
};

loadNames();

// Watch the JSON file for changes
const watcher = chokidar.watch(ITEM_LIST_PATH);

// When the JSON file changes, re-read and update the Set of item names.
watcher.on('change', path => {
    loadNames();
});

const countListingsForItem = async (name) => {
    try {
        const result = await db.one(`
            SELECT 
                COUNT(*) FILTER (WHERE intent = 'sell') AS sell_count,
                COUNT(*) FILTER (WHERE intent = 'buy') AS buy_count
            FROM listings
            WHERE name = $1;
        `, [name]);

        const { sell_count, buy_count } = result;
        return sell_count >= 1 && buy_count >= 10;
    } catch (error) {
        console.error("Error counting listings:", error);
        throw error;
    }
};

const updateFromSnapshot = async (name, sku) => {
    // Check if always call snapshot API setting is enabled.
    if (!alwaysQuerySnapshotAPI) {
        // Check if required number of listings already exist in database for item.
        // If not we need to query the snapshots API.
        let callSnapshot = await countListingsForItem(name);
        if(callSnapshot) {
            // Get listings from snapshot API.
            let unformatedListings = await Methods.getListingsFromSnapshots(name);
            // Insert snapshot listings.
            await insertListings(unformatedListings, sku, name);
        }
    } else {
        // Get listings from snapshot API.
        let unformatedListings = await Methods.getListingsFromSnapshots(name);
        // Insert snapshot listings.
        await insertListings(unformatedListings, sku, name);
    }
}

const insertListings = async (unformattedListings, sku, name) => {
    // If there are no listings from the snapshot just return. No update can be done.
    if (!unformattedListings) {
        throw new Error(`No listings found for ${name} in the snapshot. The name: '${name}' likely doesn't match the version on the items bptf listings page.`);
    }
    try {
        let formattedListings = [];
        const uniqueSet = new Set(); // Create a set to store unique combinations

        // Calculate timestamp once, no point doing it for each iteration.
        // We take 60 seconds from the timestamp as snapshots results can be up to a minute old.
        // This allows us to essentially prioritise keeping the listings in the database that
        // were added by the websocket. As we know they are the most up-to-date.
        let updated = Math.floor(Date.now() / 1000) - 60;

        for (const listing of unformattedListings) {
            if (
                listing.details &&
                excludedListingDescriptions.some(detail =>
                    listing.details.normalize('NFKD').toLowerCase().trim().includes(detail)
                )
            ) {
                // Skip this listing. Listing is for a spelled item.
                continue;
            }

            // The item object where paint and stuff is stored.
            const listingItemObject = listing.item;

            // Filter out painted items.
            if (listingItemObject.attributes && listingItemObject.attributes.some(attribute => {
                return typeof attribute === 'object' && // Ensure the attribute is an object.
                    attribute.float_value &&  // Ensure the attribute has a float_value.
                    // Check if the float_value is in the blockedAttributes object.
                    Object.values(blockedAttributes).map(String).includes(String(attribute.float_value)) &&
                    // Ensure the name of the item doesn't include any of the keys in the blockedAttributes object.
                    !Object.keys(blockedAttributes).some(key => name.includes(key));
            })) {
                continue;  // Skip this listing. Listing is for a painted item.
            }

            // If userAgent field is not present, continue.
            // This indicates that the listing was not created by a bot.
            if (!listing.userAgent) {
                continue;
            }

            // Create a unique key for each listing comprised
            // of each key of the composite primary key.
            const uniqueKey = `${listing.steamid}-${name}-${sku}-${listing.intent}`;
            // Check if this combination is already in the uniqueSet
            if (uniqueSet.has(uniqueKey)) {
                // Duplicate entry, skip this listing.
                continue;
            }
            // Add the unique combination to the set
            uniqueSet.add(uniqueKey);

            let formattedListing = {};
            // We set name to what we know it should be.
            formattedListing.name = name;
            // We set the sku to what we generated.
            formattedListing.sku = sku;
            let currenciesValid = Methods.validateObject(listing.currencies);
            // If currencies is invalid.
            if (!currenciesValid) {
                // Skip this listing.
                continue;
            }
            let validatedCurrencies = Methods.createCurrencyObject(listing.currencies);
            formattedListing.currencies = JSON.stringify(validatedCurrencies);
            formattedListing.intent = listing.intent;
            formattedListing.updated = updated;
            formattedListing.steamid = listing.steamid;

            formattedListings.push(formattedListing);
        }

        if (formattedListings.length > 0) {
            // Bulk insert rows into the table using pg-promise module.
            // I only update the listings if the updated timestamp is greater than the current one.
            // We do this to ensure that we don't overwrite a newer listing with an older one.
            const query =
                pgp.helpers.insert(formattedListings, cs, 'listings') +
                ` ON CONFLICT (name, sku, intent, steamid)\
                DO UPDATE SET currencies = excluded.currencies, updated = excluded.updated\
                WHERE excluded.updated > listings.updated;`;

            await db.none(query);
        }
        return;
    } catch (e) {
        console.log(e);
        throw e;
    }
};

// Arguably quite in-efficient but I don't see a good alternative at the moment.
// 35 minutes old (or older) listings are removed.
// Every 30 minutes listings on backpack.tf are bumped, given you have premium, which the majority of good bots do.
// So, by setting the limit to 35 minutes it allows the pricer to catch those bump events and keep any related
// listings in our database.

// Otherwise, if the listing isn't bumped or we just don't recieve the event, we delete the old listing as it may have been deleted.
// Backpack.tf may not have sent us the deleted event etc.
const deleteOldListings = async () => {
    return await db.any(`DELETE FROM listings WHERE EXTRACT(EPOCH FROM NOW() - to_timestamp(updated)) >= 2100;`);
};

const calculateAndEmitPrices = async () => {
    stats.custom = 0; // Reset stats
    stats.pricestf = 1; // Reset stats
    var completed = 0;
    console.log(`| SNAPSHOT |: Snapshotting ${allowedItemNames.size} items.`);
    for (const name of allowedItemNames) {
        // Get sku of item via the item name.
        let sku = schemaManager.schema.getSkuFromName(name);
        // Delete old listings from database.
        await deleteOldListings();
        // Use snapshot API to populate database with listings for item.
        await updateFromSnapshot(name, sku);
        completed++;
        console.log(`| SNAPSHOT |: IN PROGRESS\nItems left : ${allowedItemNames.size - completed}\nCompleted  : ${completed}`);
    }
    console.log(`| SNAPSHOT |: COMPLETE\nCompleted  : ${completed}`);
    runPricerDelay(); // Begin loop once again
};

function runPricerDelay() { // Runs the pricer every defined amount of minutes
    console.log(`| TIMER |: Running snapshot-worker again in ${config.priceTimeoutMin} minute(s).`);
    setTimeout(async () => {
        await calculateAndEmitPrices();
    }, config.priceTimeoutMin * 60 * 1000);
}

// When the schema manager is ready we proceed.
schemaManager.init(async function(err) {
    if (err) {
        throw err;
    }
    await calculateAndEmitPrices();
});