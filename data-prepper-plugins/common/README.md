# Common Data Prepper plugins

## `string_coverter`

A prepper plugin to generate new string records with upper or lower case conversion on the content of input records.

- upper_case (boolean): convert to upper case if true; otherwise convert to lower case

## `file` (source)

A source plugin to read input data from the specified file path. The file source creates a new Record for each line of data in the file.

* `path` (String): absolute input data file path. It is required
  
* `write_timeout` (int): The amount of time to attempt writing a Record to the Buffer before timing out. Unit is milliseconds and default is `5,000`  
  
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
            
  
  
* `type` (String): The Event type that will be stored in the metadata of the Event. Default is `event`. 

## `file` (sink)

A sink plugin to write output data to the specified file path.

- path (String): absolute output file path

## `stdin`

A source plugin to read input data from console.

## `stdout`

A sink plugin to write output data to console.
