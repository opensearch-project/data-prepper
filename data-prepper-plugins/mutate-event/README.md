# Mutate Event Processor
This is a processor that takes in an event and performs a single action upon it 

## Basic Usage
To get started, create the following `pipeline.yaml`.
```yaml
kv-pipeline:
  source:
    file:
      path: "/full/path/to/logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - mutate_event:
        add:
          "newMessage": "newData"
  sink:
    - stdout:
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"message": "data"}
```

When run, the processor will parse the message into the following output:

```json
{"message": "data", "newMessage": "newData"}
```

##Mutation Actions
There must be exactly one mutation action defined in the configuration.
* `add` - Takes in a new event name and its associated data
* `copy`- Takes in the source event name and the destination event name
* `rename` - Takes in a source event name and the destination event name
* `delete` - Takes in the event name to delete

##Configuration
* `overwrite` - When set to true, the mutation action will overwrite any existing data. Otherwise, no action is performed if there is existing data.
  * Default is `true`

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/readme/monitoring.md)
