# Skybrake Data GTFS Sync

Skybrake Data GTFS Sync is a Github actions repo.We fetch the static data from Vancouver every 5 AM Saturday.
The data is then uploaded to a Firebase Storage where it can be used later with apps.

The Data is converted into a protobuf file for compression and ease of storage.

## Usage

Fork/Copy project into your own folder.

Replace Secrets with your own Firebase Storage Data and setup Storage.

If running locally instead of on GitHub actions, please setup your .env instead with the proper naming.