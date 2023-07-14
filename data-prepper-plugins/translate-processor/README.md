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
              key: "result"
              map:
                404: "Not Found"
  sink:
    - stdout:
    
```

Create the following file named `logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```json
{"status": "404"}
```

The translate processor configuration from `pipeline.yaml` will retrieve the `source` value from the event and checks it against the map provided under the targets.
If a match is found, the mapped value will be placed in the target key provided.

When you run the Data Prepper with this `pipeline.yaml` passes in, you should see the following standard output.
```json
{
  "status": "404",
  "result": "Not Found"
}
```


## Configuration Options 
* `source`: This is a mandatory option to specify the field in the incoming event that needs to be translated.   
    The source option can accept either: 
    * `String` : Takes a field name and the processor will process the value in that field for translation based on the defined mappings.
    * `List<String>` : When a list of fields is provided, all the items in the list will be translated, and the corresponding target values will be placed in an array in the target field. <br><br> 
  
* `target`: Takes a `string` to specify the field in the output where the translated value should be placed.
  * Type: String
  * Required: Yes  <br><br>
* `map`: To configure a lookup table with key-value pairs for translation. By specifying the map, you define the translations that should occur.
  The processor matches the source value with the map and retrieves the corresponding translated values to populate the target.  
  * Type : Dictionary     
  * Required: No (`patterns` should be configured if `map` is not provided)<br><br>
  
  Given the setup from [Basic Usage](#basic-usage), modify the `pipeline.yaml` translate configuration to the following:
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
  Note that the `source` option is configured with list of fields.

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
  More details on how `map` can be configured can be found [here](#configuring-map-option)<br><br>
* `default`: This option allows you to specify what the target field should be populated with, when no match is found during translation.  
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
* `target_type`: This option is used to specify the type of data you want for the target value. You can choose between `"integer"`, `"double"`, `"boolean"`, and `"string"`.
If you don't provide a `target_type`, the default assumption is that the target value is a `"string"`.  
  * Type: String
  * Required: No
  ```yaml
    processor:
      - translate:
          mappings:
            - source: "status"
              targets:
                - target: "result"
                  target_type: "boolean"
                  map:
                    "120" : True
    ```
  _When configuring the `target_type` option, ensure that all the values provided under `map` and `patterns` are of the specified data type._<br><br>

* `translate_when`: This option is used to specify a conditional statement, and translations will only occur if the statement is true.
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
  With this configuration, translations will only occur if the result field is not present in the incoming event. If the result field is already present, the translations will be skipped.<br><br>

* `patterns`: The patterns option can take regex patterns as keys and map it with respective values. 
This option falls under the regex option. patterns is mandatory while configuring regex.
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
                    "2[0-9]{2}" : "Success"
                    "4[0-9]{2}": "Error"
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

* `exact`: If the value of `exact` is set to `true`, the processor will only translate keys that exactly match the source value. 
On the other hand, if `exact` is set to `false`, the processor will translate keys that are substrings to source value.
Non-exact translations does not apply for the mappings provided under `map` option.
  * Type: Boolean
  * Required: No
  * Default: True
  ```yaml
    processor:
      - translate:
          mappings:
            - source: "sourceField"
              targets:
                - target: "targetField"
                  regex:
                    exact: False
                    patterns:
                      "foo" : "bar"
    ```
  Let the contents of `logs_json.log` be the following:
    ```json
    {
      "sourceField": "footer"
    }
    ```
  The translated log would look like this:
    ```json
    {
      "sourceField": "footer",
      "result": "bar"
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
