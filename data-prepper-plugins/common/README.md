# Common Data Prepper plugins

## `string_coverter`

A prepper plugin to generate new string records with upper or lower case conversion on the content of input records.

- upper_case (boolean): convert to upper case if true; otherwise convert to lower case

## `file` (source)

A source plugin to read input data from the specified file path. The file source creates a new Record for each line of data in the file.

* `path` (String): absolute input data file path. It is required

* `format` (String): The format of each line of the file. Valid options are `json` or `plain`. Default is `plain`.
  <br></br>
    * `plain`: Reads plaintext data from files. Internally, a plain text line from a file will be given a key of `message` as shown below.
    ```
    Example log line in file
    ```
  becomes 
    ```
    { "message": "Example log line in file" }
    ```
  
    * `json`: Reads data that is in the form of a JSON string from a file. If the json string is unable to be parsed, the file source will treat it as a plaintext line.
  Expects json lines as follows:
      ```
      { "key1": "val1" }
      { "key2": "val2" }
      { "key3": "val3" }
      ```
      
* `record_type` (String): The Event type that will be stored in the metadata of the Event. Default is `string`. 
Temporarily, `type` can either be `event` or `string`. If you would like to use the file source for log analytics use cases like grok, 
  change this to `event`.

## `file` (sink)

A sink plugin to write output data to the specified file path.

- path (String): absolute output file path
- append (Boolean): set to `true` if the file should be opened for write in append mode. `false` otherwise. Default is `false`

## `stdin`

A source plugin to read input data from console. The `stdin` source creates a new Record for each input line from console 
until `exit` line. Internally, each input line before `exit` line will be given a key of
`message` as shown below.
```
Example log line 1 from console
Example log line 2 from console
exit
```
becomes 
```
{ "message": "Example log line 1 from console" }
{ "message": "Example log line 2 from console" }
```

## `random`

A source plugin that auto-generate new line of random UUID string data until stop. The `random` source creates a new 
Record for each generated line of UUID. Internally, each line will be given a key of `message` as shown below.
```
{ "message": "<UUID>" }
```

## `stdout`

A sink plugin to write output data to console.
