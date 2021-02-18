# Common Data Prepper plugins

## `string_coverter`

A prepper plugin to generate new string records with upper or lower case conversion on the content of input records.

- upper_case (boolean): convert to upper case if true; otherwise convert to lower case

## `file` (source)

A source plugin to read input data from the specified file path.

- path (String): absolute input data file path

## `file` (sink)

A sink plugin to write output data to the specified file path.

- path (String): absolute output file path

## `stdin`

A source plugin to read input data from console.

## `stdout`

A sink plugin to write output data to console.