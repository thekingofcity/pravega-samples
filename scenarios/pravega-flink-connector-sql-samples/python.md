# Flink Pravega Table API samples for Python

This module contains examples to demonstrate the use of Flink connector Table API and it uses [NY Taxi Records](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml) to demonstrate the usage.
```
The following files are downloaded and used for this sample application
https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-01.csv
https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

## Pre-requisites
1. Pravega running (see [here](http://pravega.io/docs/latest/getting-started/) for instructions)
2. Build the [pravega-samples](https://github.com/pravega/pravega-samples) by following the [instructions](https://github.com/pravega/pravega-samples#pravega-samples-build-instructions)
3. Python and its packages, see [pyflink setup](https://ci.apache.org/projects/flink/flink-docs-stable/docs/dev/python/installation/)
4. Get the latest Pravega Flink connectors at [the release page](https://github.com/pravega/flink-connectors/releases)

## Running the samples

After building the samples, navigate to the application install location.
> cd $SAMPLES-HOME/scenarios/pravega-flink-connector-sql-samples/build/install/pravega-flink-connector-sql-samples

Usage:

```
flink run --python ./bin/<popular_destination_query.py|popular_taxi_vendor.py|max_travellers_per_destination.py> --pyFiles common.py

Additional optional parameters: --scope <scope-name> --stream <stream-name> --controller-uri <controller-uri>
```

1. Load taxi data to Pravega

    The default option assumes that Pravega is running locally. You can override it by passing the controller URI options. The `create-stream` option allows the program to create the scope and stream (enabled by default).

    > bin/tableapi-samples --runApp Prepare

    The above command loads the taxi dataset records to Pravega and prepares the environment for testing.

2. Popular Destination

    > flink run --python ./bin/popular_destination_query.py --pyFiles ./bin/common.py --jarfile ~/pravega-connectors-flink-1.11_2.12-0.9.1.jar

    The above command uses SQL to find the most popular destination (drop-off location) from the available trip records.

3. Popular Taxi Vendors

    > flink run --python ./bin/popular_taxi_vendor.py --pyFiles ./bin/common.py --jarfile ~/pravega-connectors-flink-1.11_2.12-0.9.1.jar

    The above command uses Table API to find the most popular taxi vendor that was used by the commuters.

4. Maximum Travellers/Destination

    > flink run --python ./bin/max_travellers_per_destination.py --pyFiles ./bin/common.py --jarfile ~/pravega-connectors-flink-1.11_2.12-0.9.1.jar

    The above command uses Table API to group the maximum number of travellers with respect to the destination/drop-off location. 