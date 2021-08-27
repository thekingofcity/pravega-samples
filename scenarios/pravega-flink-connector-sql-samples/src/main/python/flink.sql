CREATE TABLE zone_lookup (
    locationId INT,
    borough STRING,
    zone STRING,
    service STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///home/thekingofcity/taxi_zone_lookup.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true'
) TBLPROPERTIES (
  'lookup.join.cache.ttl' = '12 h'
);

CREATE TABLE source (
    vendorId INT,
    pickupTime TIMESTAMP(3),
    dropOffTime TIMESTAMP(3),
    passengerCount INT,
    tripDistance DECIMAL,
    rateCodeId INT,
    storeAndFwdFlag STRING,
    startLocationId INT,
    destLocationId INT,
    column10 INT,
    column11 INT,
    column12 INT,
    column13 DECIMAL,
    column14 DECIMAL,
    column15 DECIMAL,
    column16 DECIMAL,
    column17 DECIMAL
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///home/thekingofcity/yellow_tripdata_2018-01-segment.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true'
);

CREATE TABLE sink (
    -- rideId INT,
    vendorId INT,
    pickupTime TIMESTAMP(3),
    dropOffTime TIMESTAMP(3),
    passengerCount INT,
    tripDistance DECIMAL,
    startLocationId INT,
    destLocationId INT,
    startLocationBorough STRING,
    startLocationZone STRING,
    startLocationServiceZone STRING,
    destLocationBorough STRING,
    destLocationZone STRING,
    destLocationServiceZone STRING
    -- PRIMARY KEY (rideId) NOT ENFORCED
) WITH (
    'connector' = 'pravega',
    'controller-uri' = 'tcp://127.0.0.1:9090',
    'scope' = 'taxi',
    'sink.stream' = 'trip',
    'format' = 'json'
);

INSERT INTO sink SELECT
    s.vendorId,
    s.pickupTime,
    s.dropOffTime,
    s.passengerCount,
    s.tripDistance,
    s.startLocationId,
    s.destLocationId,
    z1.borough,
    z1.zone,
    z1.service,
    z2.borough,
    z2.zone,
    z2.service
FROM source as s
    JOIN zone_lookup AS z1
        ON z1.locationId = s.startLocationId
    JOIN zone_lookup AS z2
        ON z2.locationId = s.destLocationId;
