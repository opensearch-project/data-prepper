# Date Processor

Date processor is used for adding default timestamp to event and for parsing date fields and using it as Data Prepper event timestamp.

## Basic Usage
To get started with date processor, create the following `pipeline.yaml`.

```yaml
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:          
    - date:
        match:
          - key: timestamp
            patterns: ["dd/MMM/yyyy:HH:mm:ss"] 
        destination: "@timestamp"
        source_timezone: "America/Los_Angeles"
        destination_timezone: "America/Chicago"
        locale: "en_US"
  sink:
    - stdout:
```
Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"timestamp": "10/Feb/2000:13:55:36"}
```

The date processor configuration from `pipeline.yaml` will parse the timestamp key from input by converting it to [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format.

When you run Data Prepper with this `pipeline.yaml` passed in, you should see the following standard output.
```json
{
  "timestamp":"10/Feb/2000:13:55:36",
  "@timestamp":"2000-02-10T15:55:36.000-06:00"
}
```

## Configuration

* `from_time_received`: A boolean that is used for adding default timestamp to the event which is when Data Prepper source first receives 
  the event. It takes the timestamp from event metadata and stores it in destination. In the absence of this option, Data Prepper doesn't 
  add a default timestamp field to any event.
  
  * Type: boolean
  * Default: false

  The following example of date configuration will use timestamp when the event is first seen by Data Prepper source and uses it as a 
  default timestamp for the event and stores it in `@timestamp`.
```yaml
processor:
  - date:
      from_time_received: true
      destination: "@timestamp"
```

* `match`: A list of `key` and `patterns` which specifies key of record to match patterns against. List can only have one with a 
valid key and at least one pattern is required if match is configured.
  * Type: List
  * Default: no default value
    * `key`: key of record to match patterns against.
      * Type: String
    * `patterns`: List of possible patterns the timestamp value of key can have. The patterns are based on sequence of letters and symbols. 
      The `patterns` support all the patterns listed in Java 
      [DatetimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html).
      and also supports `epoch_second`, `epoch_milli` and `epoch_nano` values which represents the timestamp as the number of seconds, milliseconds and nano seconds since epoch. Epoch values are always UTC time zone.
      * Type: `List<String>`

The following example of date configuration will use `timestamp` key to match against given patterns and stores the timestamp in ISO 8601
format in destination. 
```yaml
processor:
  - date:
      match:
        - key: timestamp
          patterns: ["dd/MMM/yyyy:HH:mm:ss", "MM/dd/yyyy"]
```
> :warning: `from_time_received` and `match` are mutually exclusive. Either use `from_time_received` or `match` but configuring both will
> throw an `InvalidPluginConfigurationException`. Use multiple date processors if both options should be used.
* `destination` (Optional): Field to store the timestamp parsed by date processor. It can be used with both `match` and `from_time_received`.
  * Type: String
  * Default: `"@timestamp"`
  
> Ingesting continuously generated logs into Opensearch with `@timestamp` field in events will reduce the effort required by user when 
> creating index template for [Data Streams](https://opensearch.org/docs/latest/opensearch/data-streams/#step-1-create-an-index-template). 
* `source_timezone` (Optional): Timezone used for parsing dates. It will be used in case of zone or offset cannot be extracted from value. 
  If zone or offset is part of the value timezone will be ignored. 
  All the zone rules are provided to JVM by default provider defined by
  IANA Time Zone Database (TZDB). Find all the available timezones [here](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List) 
  in "TZ database name" column.
  * Type: String
  * Default value: system default

* `destination_timezone` (Optional): Timezone used for storing timestamp in `destination` field. The available timezone values are the same as `source_timestamp`.
  * Type: String
  * Default: system default

* `locale` (Optional): Locale is used for parsing dates. It can have language, country and variant fields using IETF BCP 47 or String 
  representation of [Locale](https://docs.oracle.com/javase/8/docs/api/java/util/Locale.html) object. For example `en-US` for IETF BCP 47 and 
  `en_US ` for string representation of Locale.
  Full list of locale fields which includes language, country and variant can be found [here](https://www.iana.org/assignments/language-subtag-registry/language-subtag-registry).
    * Type: String
    * Default: `Locale.ROOT`

* `to_origination_metadata` (Optional): When this option is used, matched time is put into the event's metadata as an instance of `Instant`.

* `output_format` (Optional): indicates the format of the `@timestamp`. Default is `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`.

## Metrics

* `dateProcessingMatchSuccessCounter`: Number of records that match with at least one pattern specified in match configuration option.
* `dateProcessingMatchFailureCounter`: Number of records that did not match any of the patterns specified in patterns match configuration option.

## Developer Guide
This plugin is compatible with Java 14. See
* [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
* [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
