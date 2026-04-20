require("dotenv").config();
const admin = require("firebase-admin");
const fetch = require("node-fetch");
const AdmZip = require("adm-zip");
const { parse } = require("csv-parse/sync");
const protobuf = require("protobufjs");
const fs = require("fs");


admin.initializeApp({
    credential: admin.credential.cert({
        projectId: process.env.FIREBASE_PROJECT_ID,
        clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
        privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n'),
    }),
    storageBucket: `${process.env.FIREBASE_PROJECT_ID}.appspot.com`
});

const db = admin.firestore();
const bucket = admin.storage().bucket();

const SKYTRAIN_ROUTE_IDS= new Set(["30053", "30052", "13686"]);
const GTFS_URL=`https://gtfs-static.translink.ca/gtfs/google_transit.zip`;

async function uploadProtoToStorage(filename, buffer){
    fs.writeFileSync(filename, buffer);

    await bucket.upload(filename, {
        destination: `gtfs/${filename}`,
        metadata: { contentType: 'application/octet-stream' }
    });

    fs.unlinkSync(filename);
    console.log(`Uploaded ${filename} to FirebaseStorage`);
}

async function main(){
    console.log("Fetching GTFS zip...");

    // Acquire zip data
    const res = await fetch(GTFS_URL);
    const buffer = await res.arrayBuffer();
    const zip = new AdmZip(Buffer.from(buffer));

    const root = await protobuf.load("gtfs.proto");

    // parse trips.txt
    const tripsCSV = zip.readAsText("trips.txt");
    const allTrips = parse(tripsCSV, {columns: true, skip_empty_lines: true});

    // Filter for only SkyTrain
    const skyTrainTrips = allTrips.filter(t => SKYTRAIN_ROUTE_IDS.has(t.route_id));
    const skyTrainTripIds = new Set(skyTrainTrips.map(t => t.trip_id));

    console.log(`Kept ${skyTrainTrips.length} / ${allTrips.length} trips`);

    const TripList = root.lookupType("TripList");
    const tripsPayload = TripList.create({ trips: skyTrainTrips.map(st => ({
            tripId: st.trip_id,
            routeId: st.route_id,
            serviceId: st.service_id,
            directionId: parseInt(st.direction_id) || 0,
        }))});
    await uploadProtoToStorage("trips.pb", TripList.encode(tripsPayload).finish());

    // stop_times.txt
    const stopTimesCSV = zip.readAsText("stop_times.txt");
    const allStopTimes = parse(stopTimesCSV, {columns: true, skip_empty_lines: true});

    const skyTrainStopTimes = allStopTimes.filter(st => skyTrainTripIds.has(st.trip_id));
    const skyTrainStopIds = new Set(skyTrainStopTimes.map(st => st.stop_id));

    console.log(`Kept ${skyTrainStopTimes.length} / ${allStopTimes.length} stops`);

    const StopTimeList = root.lookupType("StopTimeList");
    const stopTimesPayload = StopTimeList.create({ stopTimes: skyTrainStopTimes.map(st => ({
            tripId: st.trip_id,
            stopId: st.stop_id,
            arrivalTime: st.arrival_time,
            departureTime: st.departure_time,
            stopSequence: parseInt(st.stop_sequence) || 0,
        }))});
    await uploadProtoToStorage("stop_times.pb", StopTimeList.encode(stopTimesPayload).finish());

    // stops.txt
    const stopsCSV = zip.readAsText("stops.txt");
    const allStops = parse(stopsCSV, {columns: true, skip_empty_lines: true});

    const skyTrainStops = allStops.filter(s => skyTrainStopIds.has(s.stop_id))
    console.log(`Kept ${skyTrainStops.length} / ${allStops.length} stops`);

    const StopList = root.lookupType("StopList");
    const stopsPayload = StopList.create({stops: skyTrainStops.map(st => ({
            stopId: st.stop_id,
            stopName: st.stop_name,
            stopLat: parseFloat(st.stop_lat) || 0,
            stopLon: parseFloat(st.stop_lon) || 0,
        }))});

    await uploadProtoToStorage("stop_times.pb", StopTimeList.encode(stopsPayload).finish());

    console.log("Sync Complete");
}

main().catch(console.error);