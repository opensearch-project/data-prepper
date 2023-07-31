# GeoIP Processor

This is the Data Prepper GeoIP processor plugin which can enrich Data Prepper events with location information using a provided IP address.
Additionally, this plugin should be able to use either a MaxMind GeoIP Lite2 database or the GeoIP2 Commercial Licensing database. 
The Data Prepper author must provide information for configuring the commercial license.


## Usages

The GeoIP processor should be configured as part of Data Prepper pipeline yaml file.

## Configuration Options

```
pipeline:
  ...
  processor:
    - geoip:
        aws:
          region: us-east-1
          sts_role_arn: arn:aws:iam::123456789012:role/Data-Prepper                  
        keys:
          - key:
              source: ["/peer/ips/src_ip1""/peer/ips/dst_ip1"]
              target: ["target1","target2"]
          - key:
              source: [ "/peer/ips/src_ip2"]
              target: [ "target3"]
              attributes: ["city_name","country_name"]
        service_type:
          maxmind:
            database_path:
              - url: 
            load_type: "in_memory"
            cache_size: 4096
            cache_refresh_schedule: P15D
```

## AWS Configuration

- `region` (Optional) : The AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html).

- `sts_role_arn` (Optional) : The AWS STS role to assume for requests to S3. which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html). 

## Properties Configuration

- `keys` (Required) : List of properties like source, target and attributes can be specified where the location fields are written

  - `source` (Required) : source  and destination IP's for which enrichment will be done. Public IP's can be either IPV4 or IPV6.

  - `target` (Required) : Properties used to specify the key for the enriched fields. 

  - `attributes` (Optional) : Used to specify the properties which are included in the enrichment of data. By default all attributes are considered.  

## Service type Configuration

- `database_path` (Required) :  Used to provide either S3 path, maxmind URL or local file path where the .mmdb file is available.

  - `url` (Required) : Provide URL for all three S3, maxmind URL or local file path. 

- `load_type` (Required) :  Load type used for better performance while enrich the data. There are two type load_type are present i.e "memory_map" or "cache".

- `cache_size` (Optional) : Used to mention the cache size. Default cache size is 2MB. Cache size applicable when load_type is cache. 

- `cache_refresh_schedule` (Required) : Switch the DatabaseReader when ever Refresh schedule threshold is met. 

- `tags_on_source_not_found` (Optional): A `List` of `String`s that specifies the tags to be set in the event the processor fails to parse or an unknown exception occurs while parsing. This tag may be used in conditional expressions in other parts of the configuration

## Sample JSON input:

{
"peer": {
"ips": {
"src_ip1": "8.8.8.8",
"dst_ip1": "8.8.8.9"
},
"host": "example.org"
},
"status": "success"
}

## Sample JSON Output:

{
"peer": {
"ips": {
"src_ip": "8.8.8.8",
"dst_ip": "8.8.8.9"
},
"host": "example.org"
},
"status": "success",
"target1": {
"continent_name": "North America",
"country_iso_code": "US",
"timezone": "America/Chicago",
"ip": "8.8.8.8",
"country_name": "United States",
"location": {
"lon": -97.822,
"lat": 37.751
},
"organization_name": "GOOGLE",
"asn": 15169,
"network": "8.8.8.0/24"
},
"target2": {
"continent_name": "North America",
"country_iso_code": "US",
"timezone": "America/Chicago",
"ip": "8.8.8.9",
"country_name": "United States",
"location": {
"lon": -97.822,
"lat": 37.751
},
"organization_name": "GOOGLE",
"asn": 15169,
"network": "8.8.8.0/24"
}
}


## Developer Guide

This plugin is compatible with Java 11. See below

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)

The integration tests for this plugin do not run as part of the Data Prepper build.

The following command runs the integration tests:

```
./gradlew :data-prepper-plugins:geoip-processor:integrationTest -Dtests.geoipprocessor.region=<your-aws-region> -Dtests.geoipprocessor.bucket=<your-bucket>
```
