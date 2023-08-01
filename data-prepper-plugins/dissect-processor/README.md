# Dissect Processor

The Dissect processor is useful when dealing with log files or messages that have a known pattern or structure. It extracts specific pieces of information from the text and map them to individual fields based on the defined Dissect patterns.


## Basic Usage

To get started with dissect processor using Data Prepper, create the following `pipeline.yaml`.
```yaml
dissect-pipeline:
  source:
    file:
      path: "/full/path/to/dissect_logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - dissect:
        map:
          log: "%{Date} %{Time} %{Log_Type}: %{Message}"
  sink:
    - stdout:
```

Create the following file named `dissect_logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```
{"log": "07-25-2023 10:00:00 ERROR: Some error"}
```

The Dissect processor will retrieve the necessary fields from the `log` message, such as `Date`, `Time`, `Log_Type`, and `Message`, with the help of the pattern `%{Date} %{Time} %{Type}: %{Message}`, configured in the pipeline.

When you run Data Prepper with this `pipeline.yaml` passed in, you should see the following standard output.

```
{
    "log" : "07-25-2023 10:00:00 ERROR: Some error",
    "Date" : "07-25-2023"
    "Time" : "10:00:00"
    "Log_Type" : "ERROR"
    "Message" : "Some error"
}
```

The fields `Date`, `Time`, `Log_Type`, and `Message` have been extracted from `log` value.

## Configuration
* `map` (Required): `map` is required to specify the dissect patterns. It takes a `Map<String, String>` with fields as keys and respective dissect patterns as values.


* `target_types` (Optional): A `Map<String, String>` that specifies what the target type of specific field should be. By default, all the values are `string`. Target types will be changed after the dissection process.


* `dissect_when` (Optional): When a conditional statement is configured with `dissect_when`, the processor will evaluate the statement before proceeding with the dissection process.
If the statement evaluates to `true`, the processor will perform the dissection. Else, skipped.

## Field Notations

Symbols like `?, +, ->, /, &`  can be  used to perform logical extraction of data.

* **Normal Field** : The field without a suffix or prefix. The field will be directly added to the output Event.

    Ex: `%{field_name}`


* **Skip Field** : ? can be used as a prefix to key to skip that field in the output JSON.  
    * Skip Field : `%{}`  
    * Named skip field  : `%{?field_name}` 

    


* **Append Field** : To append multiple values and put the final value in the field, we can use + before the field name in the dissect pattern
    * **Usage**:

            Pattern : "%{+field_name}, %{+field_name}"
            Text : "foo, bar"  
  
            Output : {"field_name" : "foobar"}

    We can also define the order the concatenation with the help of suffix `/<digits>` .

    * **Usage**:

            Pattern : "%{+field_name/2}, %{+field_name/1}"
            Text : "foo, bar"
        
            Output : {"field_name" : "barfoo"}

    If the order is not mentioned, the append operation will take place in the order of fields specified in the dissect pattern.<br><br>

* **Indirect Field** : While defining a pattern, prefix the field with a `&` to assign the value found with this to the value of another field found.
    * **Usage**:

            Pattern : "%{?field_name}, %{&field_name}"
            Text: "foo, bar"  
    
            Output : {“foo” : “bar”}

  Here we can see that `foo` which was captured from the skip field `%{?field_name}` is made the key to value captured form the field `%{&field_name}`
    * **Usage**:
  
            Pattern : %{field_name}, %{&field_name}
            Text: "foo, bar"
    
            Output : {“field_name”:“foo”, “foo”:“bar”}

    We can also indirectly assign the value to an appended field, along with `normal` field and `skip` field.

### Padding

* `->` operator can be used as a suffix to a field to indicate that white spaces after this field can be ignored.
    * **Usage**:

            Pattern : %{field1→} %{field2}
            Text : “firstname               lastname”
  
            Output : {“field1” : “firstname”, “field2” : “lastname”}

* This operator should be used as the right most suffix.
    * **Usage**:

          Pattern : %{fieldname/1->} %{fieldname/2}

    If we use `->` before `/<digit>`, the `->` operator will also be considered part of the field name.


## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
