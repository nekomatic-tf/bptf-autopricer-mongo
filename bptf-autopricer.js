const fs = require('fs');
const chokidar = require('chokidar');

const methods = require('./methods');
const Methods = new methods();

const Schema = require('@tf2autobot/tf2-schema');

const config = require('./config.json');

const SCHEMA_PATH = './schema.json';
const PRICELIST_PATH = './files/pricelist.json';
const ITEM_LIST_PATH = './files/item_list.json';

const { listen, socketIO } = require('./API/server.js');

const { MongoClient } = require('mongodb');
const client = new MongoClient(config.mongo.uri);
var db;

// Steam API key is required for the schema manager to work.
const schemaManager = new Schema({
    apiKey: config.steamAPIKey
});

// Steam IDs of bots that we want to ignore listings from.
const excludedSteamIds = config.excludedSteamIDs;

// Steam IDs of bots that we want to prioritise listings from.
const prioritySteamIds = config.trustedSteamIDs;

// Listing descriptions that we want to ignore.
const excludedListingDescriptions = config.excludedListingDescriptions;

// Blocked attributes that we want to ignore. (Paints, parts, etc.)
const blockedAttributes = config.blockedAttributes;

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

// Pricelist doesn't exist.
if (!fs.existsSync(PRICELIST_PATH)) {
    try {
        fs.writeFileSync(PRICELIST_PATH, '{"items": []}', 'utf8');
    } catch (err) {
        console.error(err);
    }
}

// Item list doesn't exist.
if (!fs.existsSync(ITEM_LIST_PATH)) {
    try {
        fs.writeFileSync(ITEM_LIST_PATH, '{"items": []}', 'utf8');
    } catch (err) {
        console.error(err);
    }
}

// This event is emitted when the schema has been fetched.
schemaManager.on('schema', function(schema) {
    // Writes the schema data to disk.
    fs.writeFileSync(SCHEMA_PATH, JSON.stringify(schema.toJSON()));
});

var keyobj;
var external_pricelist;

const updateKeyObject = async () => {
    var key_item = await Methods.getKeyFromExternalAPI();
    // Save item to pricelist. Pricelist.json is mainly used by the pricing API.
    Methods.addToPricelist(key_item, PRICELIST_PATH);
    keyobj = {
        metal: key_item.sell.metal
    };
    // Emit new key price.
    socketIO.emit('price', key_item);
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

const calculateAndEmitPrices = async () => {
    let item_objects = [];
    stats.custom = 0; // Reset stats
    stats.pricestf = 1; // Reset stats
    var completed = 0;
    console.log(`| STATUS |: Items to price: ${allowedItemNames.size}`);
    for (const name of allowedItemNames) {
        try {
            // We don't calculate the price of a key here.
            if (name === 'Mann Co. Supply Crate Key') {
                completed++;
                stats.pricestf++; // Always set key as if it was priced by prices.tf
                continue;
            }
            // Get sku of item via the item name.
            let sku = schemaManager.schema.getSkuFromName(name);
            // Start process of pricing item.
            let arr = await determinePrice(name, sku);
            let item = finalisePrice(arr, name, sku);
            // If the item is undefined, we skip it.
            if (!item) {
                continue;
            }
            // Save item to pricelist. Pricelist.json is mainly used by the pricing API.
            Methods.addToPricelist(item, PRICELIST_PATH);
            console.log(`| PRICER |: Priced ${name}.`);
            // Instead of emitting item here, we store it in a array, so we can emit all items at once.
            // This allows us to control the speed at which we emit items to the client.
            // Up to your own discretion whether this is neeeded or not.
            item_objects.push(item);
            stats.custom++;
        } catch (e) { // Fallback to prices.tf price.
            console.log(`${e.toString()}\n| PRICER |: Failed to price ${name}, using prices.tf.`);
            let item = Methods.getItemPriceFromExternalPricelist(schemaManager.schema.getSkuFromName(name), external_pricelist);
            // Instead of emitting item here, we store it in a array, so we can emit all items at once.
            // This allows us to control the speed at which we emit items to the client.
            // Up to your own discretion whether this is neeeded or not.
            item_objects.push(item['pricetfItem']);
            stats.pricestf++;
        }
        completed++;
        console.log(`| STATUS |: PRTCING\nItems left : ${allowedItemNames.size - completed}\nCompleted  : ${completed}\nItems priced with pricer    : ${stats.custom}\nItems prices with prices.tf : ${stats.pricestf}`);
    }
    // Emit all items within extremely quick succession of eachother.
    // With a 0.3 second gap between each.
    var socket_remaining = item_objects.length;
    var socket_emitted = 0;
    for (const item of item_objects) {
        // Emit item object.
        await Methods.waitXSeconds(0.3);
        socketIO.emit('price', item);
        console.log(`| SOCKET |: STATUS\nRemaining: ${socket_remaining - socket_emitted}\nCompleted: ${socket_emitted}`);
        console.log(`| SOCKET |: Emitted price for ${item.name}.`);
        socket_emitted++;
    }
    console.log(`| STATUS |: COMPLETE\nCompleted  : ${completed}\nItems priced with pricer    : ${stats.custom}\nItems prices with prices.tf : ${stats.pricestf}`);
    runPricerDelay(); // Begin loop once again
};

function runPricerDelay() { // Runs the pricer every defined amount of minutes
    console.log(`| TIMER |: Running pricer again in ${config.priceTimeoutMin} minute(s).`);
    setTimeout(async () => {
        await calculateAndEmitPrices();
    }, config.priceTimeoutMin * 60 * 1000);
}

// When the schema manager is ready we proceed.
schemaManager.init(async function(err) {
    if (err) {
        throw err;
    }
    // Connect to MongoDB
    await client.connect();
    db = await client.db(config.mongo.db).collection('listings');
    console.log(`| MONGO |: Connected.`);
    // Update key object.
    await updateKeyObject();
    // Get external pricelist.
    external_pricelist = await Methods.getExternalPricelist();
      
    // Set-up timers for updating key-object, external pricelist and creating prices from listing data.
    // Get external pricelist every 30 mins.
    setInterval(async () => {
        try {
            external_pricelist = await Methods.getExternalPricelist();
        } catch (e) {
            console.error(e);
        }
    }, 30 * 60 * 1000);

    // Update key object every 3 minutes.
    setInterval(async () => {
        try {
            await updateKeyObject();
        } catch (e) {
            console.error(e);
        }
    }, 3 * 60 * 1000);

    /*// Calculate prices using listing data every 15 minutes.
    setInterval(async () => {
        await calculateAndEmitPrices();
    }, 15 * 60 * 1000); // Every 15 minutes.*/

    // Calculate and emit prices on startup.
    await calculateAndEmitPrices();
});

const getListings = async (name, intent) => {
    const result = await db.aggregate([
        {
            $match: {
                "name": name,
            }
        },
        {
            $project: {
                listings: {
                    $filter: {
                        input: "$listings",
                        as: "listing",
                        cond: { $eq: ["$$listing.intent", intent] }
                    }
                }
            }
        }
    ]).toArray();
    return result[0]['listings'];
};

const determinePrice = async (name, sku) => {
    var buyListings = await getListings(name, 'buy');
    var sellListings = await getListings(name, 'sell');

    // Filter out unwanted listings from the MongoDB database
    var buyListingsFiltered = buyListings.filter((listing) => {
        let steamid = listing.steamid;
        let listingDetails = listing.details; // This will decide whether or not we ignore the listings without a description in them.
        let listingItemObject = listing.item; // The item object where paint and stuff is stored.
         // Filter out painted items.
         if (listingItemObject.attributes && listingItemObject.attributes.some(attribute => {
            return typeof attribute === 'object' && // Ensure the attribute is an object.
                attribute.float_value &&  // Ensure the attribute has a float_value.
                // Check if the float_value is in the blockedAttributes object.
                Object.values(blockedAttributes).map(String).includes(String(attribute.float_value)) &&
                // Ensure the name of the item doesn't include any of the keys in the blockedAttributes object.
                !Object.keys(blockedAttributes).some(key => name.includes(key));
        })) {
            return false;  // Skip this listing. Listing is for a painted item.
        }
        if (excludedSteamIds.some(id => steamid === id)) {
            return false;
        }
        if (!listingDetails && excludedListingDescriptions.some(detail => listingDetails.normalize('NFKD').toLowerCase().trim().includes(detail))) {
            return false;
        }
        return true;
    }).map((listing) => { return listing; });

    var sellListingsFiltered = sellListings.filter((listing) => {
        let steamid = listing.steamid;
        let listingDetails = listing.details; // This will decide whether or not we ignore the listings without a description in them.
        let listingItemObject = listing.item; // The item object where paint and stuff is stored.
         // Filter out painted items.
         if (listingItemObject.attributes && listingItemObject.attributes.some(attribute => {
            return typeof attribute === 'object' && // Ensure the attribute is an object.
                attribute.float_value &&  // Ensure the attribute has a float_value.
                // Check if the float_value is in the blockedAttributes object.
                Object.values(blockedAttributes).map(String).includes(String(attribute.float_value)) &&
                // Ensure the name of the item doesn't include any of the keys in the blockedAttributes object.
                !Object.keys(blockedAttributes).some(key => name.includes(key));
        })) {
            return false;  // Skip this listing. Listing is for a painted item.
        }
        if (excludedSteamIds.some(id => steamid === id)) {
            return false;
        }
        if (!listingDetails && excludedListingDescriptions.some(detail => listingDetails.normalize('NFKD').toLowerCase().trim().includes(detail))) {
            return false;
        }
        return true;
    }).map((listing) => { return listing; });

    // Get the price of the item from the in-memory external pricelist.
    var data;
    try {
        data = Methods.getItemPriceFromExternalPricelist(sku, external_pricelist);
    } catch (e) {
        throw new Error(`| UPDATING PRICES |: Couldn't price ${name}. Issue with Price.tf.`);
    }

    var pricetfItem = data.pricetfItem;

    if (
        (pricetfItem.buy.keys === 0 && pricetfItem.buy.metal === 0) ||
        (pricetfItem.sell.keys === 0 && pricetfItem.sell.metal === 0)
    ) {
        throw new Error(`| UPDATING PRICES |: Couldn't price ${name}. Item is not priced on price.tf, therefore we can't
        compare our average price to it's average price.`);
    }

    try {
        // Check for undefined. No listings.
        if (!buyListingsFiltered || !sellListingsFiltered) {
            throw new Error(`| UPDATING PRICES |: ${name} not enough listings...`);
        }

        if (buyListingsFiltered.rowCount === 0 || sellListingsFiltered.rowCount === 0) {
            throw new Error(`| UPDATING PRICES |: ${name} not enough listings...`);
        }
    } catch (e) {
        throw e;
    }

    // Sort buyListings into descending order of price.
    var buyFiltered = buyListingsFiltered.sort((a, b) => {
        let valueA = Methods.toMetal(a.currencies, keyobj.metal);
        let valueB = Methods.toMetal(b.currencies, keyobj.metal);

        return valueB - valueA;
    });

    // Sort sellListings into ascending order of price.
    var sellFiltered = sellListingsFiltered.sort((a, b) => {
        let valueA = Methods.toMetal(a.currencies, keyobj.metal);
        let valueB = Methods.toMetal(b.currencies, keyobj.metal);

        return valueA - valueB;
    });

    // TODO filter out listings that include painted hats.

    // We prioritise using listings from bots in our prioritySteamIds list.
    // I.e., we move listings by those trusted steamids to the front of the
    // array, to be used as a priority over any others.

    buyFiltered = buyListingsFiltered.sort((a, b) => {
        // Custom sorting logic to prioritize specific Steam IDs
        const aIsPrioritized = prioritySteamIds.includes(a.steamid);
        const bIsPrioritized = prioritySteamIds.includes(b.steamid);

        if (aIsPrioritized && !bIsPrioritized) {
            return -1; // a comes first
        } else if (!aIsPrioritized && bIsPrioritized) {
            return 1; // b comes first
        } else {
            return 0; // maintain the original order (no priority)
        }
    });

    sellFiltered = sellListingsFiltered.sort((a, b) => {
        // Custom sorting logic to prioritize specific Steam IDs
        const aIsPrioritized = prioritySteamIds.includes(a.steamid);
        const bIsPrioritized = prioritySteamIds.includes(b.steamid);

        if (aIsPrioritized && !bIsPrioritized) {
            return -1; // a comes first
        } else if (!aIsPrioritized && bIsPrioritized) {
            return 1; // b comes first
        } else {
            return 0; // maintain the original order (no priority)
        }
    });

    try {
        let arr = getAverages(name, buyFiltered, sellFiltered, sku, pricetfItem);
        return arr;
    } catch (e) {
        throw e;
    }
};

const calculateZScore = (value, mean, stdDev) => {
    return (value - mean) / stdDev;
};

const filterOutliers = listingsArray => {
    // Calculate mean and standard deviation of listings.
    const prices = listingsArray.map(listing => Methods.toMetal(listing.currencies, keyobj.metal));
    const mean = Methods.getRight(prices.reduce((acc, curr) => acc + curr, 0) / prices.length);
    const stdDev = Math.sqrt(prices.reduce((acc, curr) => acc + Math.pow(curr - mean, 2), 0) / prices.length);

    // Filter out listings that are 3 standard deviations away from the mean.
    // To put it plainly, we're filtering out listings that are paying either
    // too little or too much compared to the mean.
    const filteredListings = listingsArray.filter(listing => {
        const zScore = calculateZScore(Methods.toMetal(listing.currencies, keyobj.metal), mean, stdDev);
        return zScore <= 3 && zScore >= -3;
    });

    if(filteredListings.length < 3) {
        throw new Error('Not enough listings after filtering outliers.');
    }
    // Get the first 3 buy listings from the filtered listings and calculate the mean.
    // The listings here should be free of outliers. It's also sorted in order of
    // trusted steamids (when applicable).
    var filteredMean = 0;
    for (var i = 0; i <= 2; i++) {
        filteredMean = +Methods.toMetal(filteredListings[i].currencies, keyobj.metal);
    }

    // Validate the mean.
    if (!filteredMean || isNaN(filteredMean) || filteredMean === 0) {
        throw new Error('Mean calculated is invalid.');
    }

    return filteredMean;
};

const getAverages = (name, buyFiltered, sellFiltered, sku, pricetfItem) => {
    // Initialse two objects to contain the items final buy and sell prices.
    var final_buyObj = {
        keys: 0,
        metal: 0
    };
    var final_sellObj = {
        keys: 0,
        metal: 0
    };

    try {
        if (buyFiltered.length < 3) {
            throw new Error(`| UPDATING PRICES |: ${name} not enough buy listings...`);
        } else if (buyFiltered.length > 3 && buyFiltered.length < 10) {
            var totalValue = {
                keys: 0,
                metal: 0
            };
            for (var i = 0; i <= 2; i++) {
                totalValue.keys += Object.is(buyFiltered[i].currencies.keys, undefined) ?
                    0 :
                    buyFiltered[i].currencies.keys;
                totalValue.metal += Object.is(buyFiltered[i].currencies.metal, undefined) ?
                    0 :
                    buyFiltered[i].currencies.metal;
            }
            final_buyObj = {
                keys: Math.trunc(totalValue.keys / i),
                metal: totalValue.metal / i
            };
        } else {
            // Filter out outliers from set, and calculate a mean average price in terms of metal value.
            let filteredMean = filterOutliers(buyFiltered);
            // Caclulate the maximum amount of keys that can be made with the metal value returned.
            let keys = Math.trunc(filteredMean / keyobj.metal);
            // Calculate the remaining metal value after the value of the keys has been removed.
            let metal = Methods.getRight(filteredMean - keys * keyobj.metal);
            // Create the final buy object.
            final_buyObj = {
                keys: keys,
                metal: metal
            };
        }
        // Decided to pick the very first sell listing as it's ordered by the lowest sell price. I.e., the most competitive.
        // However, I decided to prioritise 'trusted' listings by certain steamids. This may result in a very high sell price, instead
        // of a competitive one.
        if (sellFiltered.length > 0) {
            final_sellObj.keys = Object.is(sellFiltered[0].currencies.keys, undefined) ?
                0 :
                sellFiltered[0].currencies.keys;
            final_sellObj.metal = Object.is(sellFiltered[0].currencies.metal, undefined) ?
                0 :
                sellFiltered[0].currencies.metal;
        } else {
            throw new Error(`| UPDATING PRICES |: ${name} not enough sell listings...`); // Not enough
        }

        var usePrices = false;
        try {
            // Will return true or false. True if we are ok with the autopricers price, false if we are not.
            // We use prices.tf as a baseline.
            usePrices = Methods.calculatePricingAPIDifferences(pricetfItem, final_buyObj, final_sellObj, keyobj);
        } catch (e) {
            // Create an error object with a message detailing this difference.
            throw new Error(`| UPDATING PRICES |: Our autopricer determined that name ${name} should sell for : ${final_sellObj.keys} keys and 
            ${final_sellObj.metal} ref, and buy for ${final_buyObj.keys} keys and ${final_buyObj.metal} ref. Prices.tf
            determined I should sell for ${pricetfItem.sell.keys} keys and ${pricetfItem.sell.metal} ref, and buy for
            ${pricetfItem.buy.keys} keys and ${pricetfItem.buy.metal} ref. Message returned by the method: ${e.message}`);
        }

        // if-else statement probably isn't needed, but I'm just being cautious.
        if (usePrices) {
            // The final averages are returned here. But work is still needed to be done. We can't assume that the buy average is
            // going to be lower than the sell average price. So we need to check for this later.
            return [final_buyObj, final_sellObj];
        } else {
            throw new Error(`| UPDATING PRICES |: ${name} pricing average generated by autopricer is too dramatically
            different to one returned by prices.tf`);
        }
    } catch (error) {
        throw error;
    };
};

const finalisePrice = (arr, name, sku) => {
    let item = {};
    try {
        if (!arr) {
            console.log(
                `| UPDATING PRICES |:${name} couldn't be updated. CRITICAL, something went wrong in the getAverages logic.`
            );
            throw new Error('Something went wrong in the getAverages() logic. DEVELOPER LOOK AT THIS.');
            // Will ensure that neither the buy, nor sell side is completely unpriced. If it is, this means we couldn't get
            // enough listings to create a price, and we also somehow bypassed our prices.tf safety check. So instead, we
            // just skip this item, disregarding the price.
        } else if ((arr[0].metal === 0 && arr[0].keys === 0) || (arr[1].metal === 0 && arr[1].keys === 0)) {
            throw new Error('Missing buy and/or sell side.');
        } else {
            // Creating item fields/filling in details.
            // Name of the item. Left as it was.
            item.name = name;
            // Add sku to item object.
            item.sku = sku;
            // If the source isn't provided as bptf it's ignored by tf2autobot.
            item.source = 'bptf';
            // Generates a UNIX timestamp of the present time, used to show a client when the prices were last updated.
            item.time = Math.floor(Date.now() / 1000);

            // We're taking the buy JSON and getting the metal price from it, then rounding down to the nearest .11.
            arr[0].metal = Methods.getRight(arr[0].metal);
            // We're taking the sell JSON and getting the metal price from it, then rounding down to the nearest .11.
            arr[1].metal = Methods.getRight(arr[1].metal);

            // We are taking the buy array price as a whole, and also passing in the current selling price
            // for a key into the parsePrice method.
            arr[0] = Methods.parsePrice(arr[0], keyobj.metal);
            // We are taking the sell array price as a whole, and also passing in the current selling price
            // for a key into the parsePrice method.
            arr[1] = Methods.parsePrice(arr[1], keyobj.metal);

            // Calculates the pure value of the keys involved and adds it to the pure metal.
            // We use this to easily compare the listing 'costs' shortly.
            var buyInMetal = Methods.toMetal(arr[0], keyobj.metal);
            var sellInMetal = Methods.toMetal(arr[1], keyobj.metal);

            // If the buy price in metal for the listing is greater than or equal to the sell price
            // we ensure the metal is in the correct format again, and we re-use the already validated
            // key price.

            // The main point here is that we use the buy price as the selling price, adding 0.11 as a margin.
            // This way if the buy price turns out to be higher than our averaged selling price, we don't
            // get screwed in this respect.
            if (buyInMetal >= sellInMetal) {
                item.buy = {
                    keys: arr[0].keys,
                    metal: Methods.getRight(arr[0].metal)
                };
                item.sell = {
                    keys: arr[0].keys,
                    metal: Methods.getRight(arr[0].metal + 0.11)
                };
            } else {
                // If the buy price is less than our selling price, we just
                // use them as expected, sell price for sell, buy for buy.
                item.buy = {
                    keys: arr[0].keys,
                    metal: Methods.getRight(arr[0].metal)
                };
                item.sell = {
                    keys: arr[1].keys,
                    metal: Methods.getRight(arr[1].metal)
                };
            }
            // Return the new item object with the latest price.
            return item;
        }
    } catch (err) {
        // If the autopricer failed to price the item, we don't update the items price.
        return;
    }
};

listen();