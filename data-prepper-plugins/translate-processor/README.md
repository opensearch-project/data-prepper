# Translate Processor

Translate Processor is used for translation of the event data with the help of a predefined map of key value pairs.

## Basic Usage
To get started with the translate processor, create the following `pipeline.yaml`.

```yaml
translate-pipeline:
  source:
    file:
      path: "/full/path/to/grok_logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - translate:
        mappings:
          - source: "status"
            targets:
              - target: "result"
                map:
                  404: "Not Found"
  sink:
    - stdout:
    
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"status": "404"}
```

The Translate processor configuration in `pipeline.yaml` retrieves the `source` value from the event data and compares it against the keys specified under the `targets`. 
When a match is found, the processor places the corresponding mapped value into the `target` key provided in the configuration.

When you run the Data Prepper with this `pipeline.yaml` passes in, you should see the following standard output.
```json
{
  "status": "404",
  "result": "Not Found"
}
```


## Configuration Options
### Mandatory
### `source`: 
The `source` option is a mandatory option used to specify the field in the incoming event that requires translation. `source` can be indicated using full paths to the field within the event data.  
See [Example](#example-config). <br><br>
  The source option can accept either:
* `String` : Takes a field name and the processor will process the value in that field for translation based on the defined mappings.
* `List<String>` : When a list of sources is provided, all the sources in the list will be translated, and the corresponding target values will be placed in an array in the target field. See this example for list of sources<br><br>
  More details on how `source` paths work can be found [here](#configuring-source-option-with-path)
### `target`:
  The `target` option is used to specify the field in the output where the translated value should be placed.
  When the Translate processor matches a `source` value from the event against the defined mappings, it will take the corresponding mapped value and store it in the `target` field in the output.  
  See [Example](#example-config).<br>
  * Type: String
  * Required: Yes  <br><br>
### `map`:
* To configure a lookup table for translation, `map` option can be used. This is not a mandatory option, however, either the `map` option or the `patterns` option needs to be configured.  
* The map consists of key-value pairs that define the translations. Each key represents a possible value in the `source` field, and the corresponding value represents what it should be translated to.
* During the translation process, the processor checks the `source` value from the event against the keys in the map. If a match is found between the `source` value and a key in the map, the processor retrieves the corresponding translated value (the value associated with that key) from the map. 
This translated value is then populated into the specified `target` field in the output.
  * Type : Dictionary
  * Required: No (`patterns` should be configured if `map` is not provided)<br><br>
More details on how to configure the keys under the `map` can be found [here](#configuring-map-option)<br><br>

### Example Config

  * Given the setup from [Basic Usage](#basic-usage), modify the `pipeline.yaml` translate configuration to the following:
    ```yaml
    processor:
      - translate:
          mappings:
            - source: ["status", "status_id"]
              targets:
                - target: "result"
                  map:
                    ok : "Success"
                    120: "Found"
    ```
    Note that the `source` option is configured with list of fields.<br><br>

    Let the contents of `logs_json.log` be the following:
    ```json
    {
      "status": "ok",
      "status_id": "120"
    }
    ```
    The translated log would look like this:
    ```json
    {
      "status": "ok",
      "status_id": "120",
      "result": [
        "Success",
        "Found"
      ] 
    }
    ```
### Optional
### `default`: 
* This option can be configured to specify what value should be put in the `target` field when no match is found during translation. 
* It provides a fallback value for cases where there is no matching translation in the provided mappings.
    * Type: String
    * Required: No
  ```yaml
    processor:
      - translate:
          mappings:
            - source: "status"
              targets:
                - target: "result"
                  default: "Informational"
                  map:
                    "120" : "success"
    ```
### `type`:
* This option is used to specify the type of data you want for the target value. You can choose between `"integer"`, `"double"`, `"boolean"`, and `"string"`.
* If `type` not provided, the default assumption is that the target value is a `"string"`.
    * Type: String
    * Required: No
  ```yaml
    processor:
      - translate:
          mappings:
            - source: "status"
              targets:
                - target: "result"
                  type: "boolean"
                  map:
                    "120" : True
    ```
  **_When configuring the `type` option, ensure that all the values provided under `map` and `patterns` are of the specified data type._**<br><br>

### `translate_when`: 
* When a conditional statement is configured with `translate_when`, the processor will evaluate the statement before proceeding with the translation process. 
* If the statement evaluates to true, the processor will perform the translations. Else, translations are skipped for that target.
    * Type: String
    * Required: No
  ```yaml
    processor:
      - translate:
          mappings:
            - source: "status"
              targets:
                - target: "result"
                  translate_when: /result!=null
                  map:
                    "120" : True
    ```
  With this configuration, translations will only occur if the `result` field is not present in the incoming event. If the `result` field is already present, the translations will be skipped.<br><br>

### `patterns`: 
* The `patterns` option is used with the `regex` option in the Translate processor. It lets you use regex patterns as keys and map them to respective values. It's mandatory when configuring `regex` option. 

    * Type: Dictionary
    * Required: No (`map` should be configured if `patterns` is not provided) <br><br>

  Given the setup from [Basic Usage](#basic-usage), modify the `pipeline.yaml` translate configuration to the following:

  ```yaml
  processor:
    - translate:
        mappings:
          - source: "status"
            targets:
              - target: "result"
                regex:
                  patterns:
                    "2[0-9]{2}" : "Success" # Matches ranges from 200-299
                    "4[0-9]{2}": "Error"    # Matches ranges form 400-499
    ```

  Let the contents of `logs_json.log` be the following:
    ```json
    {
      "status": "404"
    }
    ```
  The translated log would look like this:
    ```json
    {
      "status": "ok",
      "result": "Error"
    }
    ```

### `exact`: 
* When the `exact` value is set to true, the processor will perform translations on source values that match the compiled pattern keys listed under `patterns`.  
* When `exact` is set to `false`, the processor will translate source values if a substring is found in the keys specified. The substring matching is based on the raw string provided as the key, and it doesn't consider the compiled pattern.
* In such cases of non-exact matches, the processor will replace the key with the corresponding mapped value, for all occurrences in the source and will be placed in the `target` specified.<br> 
* It's important to note that non-exact translations do not apply to the mappings provided under the `map` option.
    * Type: Boolean
    * Required: No
    * Default: True
  ```yaml
    translate:
      mappings:
        - source: status
          targets:
            - target: result
              regex:
                exact: false
                patterns:
                  foo: bar
        - source: status2
          targets:
            - target: result2
              regex:
                exact: false
                patterns:
                  foo: bar
    ```
  Let the contents of `logs_json.log` be the following:
    ```json
   { 
      "status" : "footer", 
      "status2" : "foofoo"
   }
    ```
  The translated log would look like this:
    ```json
   { 
      "status" : "footer", 
      "status2" : "foofoo",
      "result" : "barter", 
      "result2" : "barbar"
   }
    ```

### `file`: 
* The `file` option in the Translate processor takes a local YAML file path or an S3 object containing translation mappings. 
* Both `mappings` and `file` options can be specified together, and the processor considers the mappings from both sources for translations. 
* This provides flexibility in managing translations, to include mappings directly in the configuration or use an external file or S3 object.
  The file contents should be in the following format:
  ```yaml
  mappings:
    - source: "status"
      targets:
        - target: "result"
          map:
            "foo": "bar"
          # Other configurations
    ```

  Given the setup from [Basic Usage](#basic-usage), modify the `pipeline.yaml` translate configuration to the following:

  When providing local file:
  ```yaml
  translate:
    file: 
      name: "/full/path/to/file.yaml"
  ```

  When providing S3 object:
  ```yaml
  translate:
    file:
      name: <key_name>
      aws:
        bucket: <bucket_name>
        region: <region_name>
        sts_role_arn: <STS role ARN>
  ```

  Replace the necessary values in the configuration based on the location of your mappings file.<br><br>

  Let the contents of `logs_json.log` be the following:
  ```json
    {
      "status": "foo"
    }
  ```
  The translated log would look like this:
  ```json
    {
      "status": "foo",
      "result": "bar"
    }
  ```
  If both `mappings` and `file` options  needs to be specified, the translate config would look something like this:
  ```yaml
  - translate:
      file:
        name: "/full/path/to/file.yaml"
      mappings:
        - source: "sourceField"
          targets:
            - target: "targetField"
              map:
                "key1": "value1"
  ```
  In this example, `file` is configured with local path to YAML file.<br><br>
  #### Overlappings:  
  * In instances where the pipeline configuration and file mappings share duplicate `source` and `target` pair, the mappings specified within the pipeline configuration take precedence.

### Configuring `source` option with path
* The source provided can be full paths to the field in the event that requires translation.

  ```json
  { 
    "field1": [
      {
        "field2": {
          "field3": "value1",
          "field4": "value2"
        },
        "field3": "value2"
      },
      {
        "field4": "value3",
        "field5": "value4"
      }
    ]
  }

  ```
  In the above provided JSON, the full path of `field3` would be `field1/field2/field3`. The root path of `field3` would be `field1/field2` <br><br>
* If the source path is something like `field1/field2/<source>`, all the objects that satisfy the path will be translated based on the provided mappings and the targets fields will be placed in the field  `field1/field2/<target>`.  
* The fields (`field1`, `field2`) leading up to the source field in the path must be JSON arrays or JSON objects. It is important to note that other JSON data types cannot be iterated in this context.
* The `translate_when` expression if configured will only be applied for the JSON objects that satisfy the path `field1/field2`.
* As `source` option supports multiple sources to be configured, it is required for all the sources in the array to have a common root path.
  ```yaml
  mappings:
    - source: "sourceField"
      targets:
        - target: "targetField"
          regex:
            exact: False
            patterns:
              "foo" : "bar"
  ```

  Given the setup from [Basic Usage](#basic-usage), modify the `pipeline.yaml` translate configuration to the following:

  ```yaml
  processor:
  - translate:
      mappings:
        - source: collection/status
          targets:
            - target: result
              map:
                foo: bar
        - source: collection/status/http
          targets:
            - target: result
              map:
                120: "success"
                404: "failure"
  ```
  Let the contents of `logs_json.log` be:
  ```json
  {
    "collection": [
      {
        "status": "foo"
      },
      {
        "status": [
          {
            "http": 120
          }
        ]
      }
    ]
  }
  ```
  The translated log would look like this:
  ```json
  {
    "collection": [
      {
        "status": "foo",
        "result": "bar"
      },
      {
        "status": [
          {
            "http": 120,
            "result": "success"
          }
        ]
      }
    ]
  }

  ```

### Configuring `map` option
The keys provided in the map option can be of the following types:
* Individual keys
  ```yaml
    map:
      ok : "Success"
      120: "Found"
  ```
* Number ranges
  ```yaml
    map:
      "100-200": "Success"
      "400-499": "Error"
  ```
* Comma-delimited keys
  ```yaml
    map:
      "key1,key2,key3": "value1"
      "100-200,key4": "value2"
  ```

  _While configuring the keys in map, there shouldn't be any overlapping number ranges or duplicate keys._

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
