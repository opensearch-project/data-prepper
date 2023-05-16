# Grok Processor

This is a processor that takes unstructured data and utilizes pattern matching
to structure and extract important keys and make data more structured and queryable.

The Grok Processor uses the [java-grok Library](https://github.com/thekrakken/java-grok) internally and supports all java-grok library compatible patterns. The java-grok library is built using the `java.util.regex` regular expression library.

The full set of default patterns can be found [here](https://github.com/thekrakken/java-grok/blob/master/src/main/resources/patterns/patterns). Custom patterns can be added through either the
`patterns_definitions` or `patterns_directories` configuration settings. When debugging custom patterns, the [Grok Debugger](https://grokconstructor.appspot.com/do/match) 
can be extremely helpful.

## Basic Usage

To get started with grok using Data Prepper, create the following `pipeline.yaml`.
```yaml
grok-pipeline:
  source:
    file:
      path: "/full/path/to/grok_logs_json.log"
      record_type: "event"
      format: "json"
  processor:
    - grok:
        match:
          message: ['%{IPORHOST:clientip} \[%{HTTPDATE:timestamp}\] %{NUMBER:response_status:int}']
  sink:
    - stdout:
```

Create the following file named `grok_logs_json.log` and replace the `path` in the file source of your `pipeline.yaml` with the path of this file.

```
{"message": "127.0.0.1 198.126.12 [10/Oct/2000:13:55:36 -0700] 200"}
```

The grok configuration from the `pipeline.yaml` will match the value in the `message` key of each log for a pattern matching `%{IPORHOST:clientip} \[%{HTTPDATE:timestamp}\] %{NUMBER:response_status:int}`.
These three patterns (`IPORHOST`, `HTTPDATE`, and `NUMBER`) are default patterns. This pattern matches the format of the log in your `grok_logs_json.log` file.

When you run Data Prepper with this `pipeline.yaml` passed in, you should see the following standard output.

```
{ 
  "message":"127.0.0.1 198.126.12 [10/Oct/2000:13:55:36 -0700] 200",
  "response_status":200,
  "clientip":"198.126.12",
  "timestamp":"10/Oct/2000:13:55:36 -0700"
}
```

As you can see, the extra keys for `clientip`, `timestamp`, and `response_status` have been pulled out from the original message.

## Configuration
* `match` (Optional): A `Map<String, List<String>>` that specifies which keys of a Record to match which patterns against. Default value is `{}`

The following example match configuration will check logs for a `message` key, and if it exists, will match the value in this `message` key first against the `SYSLOGBASE` pattern, and then against the `COMMONAPACHELOG` pattern.
It will then check logs for a `timestamp` key, and if it exists, will attempt to match the value in this `timestamp` key against the `TIMESTAMP_ISO8601` pattern.
Note that by default, matching will be done until there is a successful match. So if there is a successful match against the value in the `message` key for a pattern of `SYSLOGBASE`, no attempted matching will be done 
for either the `COMMONAPACHELOG` or `TIMESTAMP_ISO8601` pattern. If you would like to match logs against every pattern in `match` no matter what, then see [break_on_match](#break_on_match).
```yaml
processor:
  - grok:
      match:
        message: ['%{SYSLOGBASE}', "%{COMMONAPACHELOG}"]
        timestamp: ["%{TIMESTAMP_ISO8601}"]  
```


* `keep_empty_captures` (Optional): A `boolean` that specifies whether `null` captures should be kept. Note that `null` captures can only occur for certain regex patterns that have the potential to match nothing, such as `.*?`. Default value is `false`


* `named_captures_only` (Optional): A `boolean` that specifies whether to only keep named captures. Default value is `true`

* `tags_on_match_failure` (Optional): A `List` of `String`s that specifies the tags to be set in the event when grok fails to match or an unknown exception occurs while matching. This tag may be used in conditional expressions in other parts of the configuration

  Named captures are those that follow the configuration of `%{SYNTAX:SEMANTIC}`. However, the `SEMANTIC` is optional, and patterns that are
  defined simply as `%{SYNTAX}` are considered unnamed captures. 
  
  Given the same setup from [Basic Grok Example](#basic-grok-example), modify the `pipeline.yaml` grok configuration to remove the `clientip` name from the `%{IPORHOST}` pattern..

```yaml
  processor:
    - grok:
        match:
          message: ['%{IPORHOST} \[%{HTTPDATE:timestamp}\] %{NUMBER:response_status:int}']
```

The resulting grokked log will now look like this.

```
{
  "message":"127.0.0.1 198.126.12 [10/Oct/2000:13:55:36 -0700] 200",
  "response_status":200,
  "timestamp":"10/Oct/2000:13:55:36 -0700"
}
```

Notice that the `clientip` key is no longer there, because the `%{IPORHOST}` pattern is now an unnamed capture.

Now set `named_captures_only` to `false` as seen below.

```yaml
  processor:
    - grok:
        match:
          named_captures_only: false
          message: ['%{IPORHOST} \[%{HTTPDATE:timestamp}\] %{NUMBER:message:int}']
```

The resulting grokked log will look like this.

```
{
  "message":"127.0.0.1 198.126.12 [10/Oct/2000:13:55:36 -0700] 200",
  "MONTH":"Oct",
  "YEAR":"2000",
  "response_status":200,
  "HOUR":"13",
  "TIME":"13:55:36",
  "MINUTE":"55",
  "SECOND":"36",
  "IPORHOST":"198.126.12",
  "MONTHDAY":"10",
  "INT":"-0700",
  "timestamp":"10/Oct/2000:13:55:36 -0700"
}
```

Note that the `IPORHOST` capture now shows up as a new key, along with some internal unnamed captures like `MONTH`, `YEAR`, etc. These patterns
are being used by the `HTTPDATE` pattern, which can be seen in the [default patterns file](https://github.com/thekrakken/java-grok/blob/master/src/main/resources/patterns/patterns).

### <a name="break_on_match"></a>
* `break_on_match` (Optional): A `boolean` that specifies whether to match all patterns from `match` against a Record, 
  or to stop once the first successful pattern match is found. Default value is `true`
  

* `keys_to_overwrite` (Optional): A `List<String>` that specifies which existing keys of a Record to overwrite if there is a capture with the same key value. Default value is `[]`

Given the same setup from [Basic Grok Example](#basic-grok-example), modify the `pipeline.yaml` grok configuration to the following:

```yaml
  processor:
    - grok:
        match:
          keys_to_overwrite: ["message"]
          message: ['%{IPORHOST:clientip} \[%{HTTPDATE:timestamp}\] %{NUMBER:message:int}']
```

Notice how `%{NUMBER:response_status:int}` has been replaced by `%{NUMBER:message:int}`, and `message` is added to the list of `keys_to_overwrite`.

The resulting grokked log will now look like this.

```
{ 
  "message":200,
  "clientip":"198.126.12",
  "timestamp":"10/Oct/2000:13:55:36 -0700"
}
```

As you can see, the original `message` key was overwritten with the `NUMBER` 200.

* `pattern_definitions` (Optional): A `Map<String, String>` that allows for custom pattern use inline. Default value is `{}`

The following grok configuration creates a custom pattern named `CUSTOM_PATTERN`, and the pattern itself is a regex pattern.

```yaml
processor:
  - grok:
      pattern_definitions:
        CUSTOM_PATTERN: 'this-is-regex'
      match:
        message: ["%{CUSTOM_PATTERN:my_pattern}"]
```

* `patterns_directories` (Optional): A `List<String>` that specifies that path of directories that contain custom pattern files you would like to use. Default value is `[]`

Creating files of custom patterns makes it easy to organize them. Consider the following directory structure.

```
patterns_folder/
  - patterns1.txt
  - patterns2.txt
extra_patterns_folder/
  - extra_patterns1.txt
```

The following grok configuration will register all patterns in `patterns1.txt`, `patterns2.txt`, and `extra_patterns1.txt`

```yaml
processor:
  - grok:
      patterns_directories: ["path/to/patterns_folder", "path/to/extra_patterns_folder"]
      match:
        message: ["%{CUSTOM_PATTERN_FROM_FILE:my_pattern}"]
```

When adding custom patterns to a file, one pattern should be declared per line. A space should separate the pattern name and its regex. The following example declares two custom patterns, `DOG` and `CAT`.

```
DOG beagle|chihuaha|retriever
CAT persian|siamese|siberian
```

* `patterns_files_glob` (Optional): A glob `String` that describes which pattern files to use from the directories specified for `patterns_directories`. Default value is `*`<br></br>

* `target_key` (Optional): A `String` that will wrap all captures for a Record in an additional outer key value. Default value is `null`


  Given the same setup from [Basic Grok Example](#basic-grok-example), modify the `pipeline.yaml` grok configuration to add a `target_key` named `grokked`

```yaml
  processor:
    - grok:
        target_key: "grok"
        match:
          message: ['%{IPORHOST} \[%{HTTPDATE:timestamp}\] %{NUMBER:response_status:int}']
```

The resulting grokked log will now look like this.

```
{ 
  "message":"127.0.0.1 198.126.12 [10/Oct/2000:13:55:36 -0700] 200",
  "grokked": {
     "response_status":200,
     "clientip":"198.126.12",
     "timestamp":"10/Oct/2000:13:55:36 -0700"
  }
}
```

All of the grok captures were wrapped in an outer key named `grokked`.<br></br>

* `timeout_millis` (Optional): An `int` that specifies the maximum amount of time, in milliseconds, that matching will be performed on an individual Record before it times out and moves on to the next Record.
Setting a `timeout_millis = 0` will make it so that matching a Record never times out. If a Record does time out, it will remain the same as it was when input to the grok processor. Default value is `30,000`

## Metrics

Counter

* `grokProcessingMismatch`: records the number of Records that did not match any of the patterns specified in the match field
  

* `grokProcessingMatch`: records the number of Records that found at least one pattern match from the match field
  

* `grokProcessingErrors`: records the total number of processing errors for Records


* `grokProcessingTimeouts`: records the total number of Records that timed out while matching

Timer

* `grokProcessingTime`: the time each individual Record takes matching against patterns from `match`. The `avg` is the most useful metric for this Timer.

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
