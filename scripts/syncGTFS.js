const admin = require("firebase-admin");
const fetch = require("node-fetch");
const AdmZip = require("adm-zip");
const { parse } = require("csv-parse/sync");

admin.initializeApp({
    credential: admin.credential.cert({
        projectId: process.env.FIREBASE_PROJECT_ID,
        clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
        privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n'),
    }),
});

const db = admin.firestore();

const SKYTRAIN_ROUTE_IDS= new Set(["99901", "99902", "99903"]);

const GTFS_URL=`https://gtfs-static.translink.ca/gtfs/google_transit.zip`;

async function uploadInBatches(collectionName, rows, keyField) {
    const BATCH_SIZE = 400;
    for(let i = 0; i <rows.length; ++i){
        const batch = db.batch();
        rows.slice(i, i + BATCH_SIZE).forEach((row) => {
           const ref = db.collection(collectionName).doc(row[keyField]);
           batch.set(ref, row);
        });

        await batch.commit();
        console.log(`Uploaded ${Math.min(i * BATCH_SIZE, rows.length)} / ${rows.length} to ${collectionName}`);
    }
}

async function main(){
    console.log("Fetching GTFS zip...");
    
    // Acquire zip data
    const res = await fetch(GTFS_URL);
    const buffer = await res.arrayBuffer();
    const zip = new AdmZip(Buffer.from(buffer));

    // parse trips.txt
    const tripsCSV = zip.readAsText("trips.txt");
    const allTrips = parse(tripsCSV, {columns: true, skip_empty_lines: true});

    // Filter for only SkyTrain
    const skyTrainTrips = allTrips.filter(t => SKYTRAIN_ROUTE_IDS.has(t.route_id));
    const skyTrainTripIds = new Set(skyTrainTrips.map(t => t.id));

    await uploadInBatches("trips", skyTrainTripIds, "trips_id");
    console.log(`Kept ${skyTrainTrips.length} / ${allTrips.length}`);

    // stop_times.txt
    const stopTimesCSV = zip.readAsText("stops_times.txt");
    const allStopTimes = parse(stopTimesCSV, {columns: true, skip_empty_lines: true});

    const skyTrainStopTimes = allStopTimes.filter(st => skyTrainTripIds.has(st.trip_id));
    const skyTrainStopIds = new Set(skyTrainStopTimes.map(st => st.stop_id));

    // stops.txt
    const stopsCSV = zip.readAsText("stops.txt");
    const allStops = parse(stopsCSV, {columns: true, skip_empty_lines: true});

    const skyTrainStops = allStops.filter(s => skyTrainStopIds.has(s.stop_id))
    console.log(`Kept ${skyTrainStops.length} / ${allStops.length} stops`);

    await uploadInBatches("stops", skyTrainStops, "stops_id");

    // stop times format
    skyTrainStopTimes.forEach(row => row._id = `${row.trip_id}_${row.stop_sequence}`);
    await uploadInBatches("stop_times", skyTrainStopIds,"_id");
    console.log(`Kept ${skyTrainStopTimes.length} / ${allStops.length} stop times`);

    console.log("Sync Complete!");
}

main().catch(console.error);