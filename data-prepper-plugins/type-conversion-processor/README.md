# Type Conversion Processor

This is a processor that converts type of specified key in a message to the specified type. Basically this is a "casting" processor that changes types of some fields in the event/message.
Some of the data in the input may need to be converted to different types (ex integer or double) for passing the events through "condition" based processors or to do conditional routing.


## Basic Usage

To get started with type conversion processor using Data Prepper, create the following `pipeline.yaml`.
```yaml
type-conv-pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - grok:
        match:
          message: ['%{IPORHOST:clientip} \[%{HTTPDATE:timestamp}\] %{NUMBER:response_status}']
    - type_conversion:
        key: "response_status"
        type: "integer"
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"message": "10.10.10.19 [19/Feb/2015:15:50:36 -0500] 200"}
```

When run, the grok processor will parse the message into the following output:
```json
{"message": "10.10.10.10 [19/Feb/2015:15:50:36 -0500] 200", "clientip":"10.10.10.10", "timestamp": "19/Feb/2015:15:50:36 -0500", "response_status": "200"}
```
and the type conversion processor will change it to the following output, where type of `response_status` value is changed to integer
```json
{"message": "10.10.10.10 [19/Feb/2015:15:50:36 -0500] 200", "clientip":"10.10.10.10", "timestamp": "19/Feb/2015:15:50:36 -0500", "response_status": 200}
```

## Configuration
* `source` - All fields in the event that will be processed
* `destination` - All fields in the event with specified field's type changed to desired type
* `key` - Name of the field in the input event whose type needs to be converted
* `type` - Target type name for the field
