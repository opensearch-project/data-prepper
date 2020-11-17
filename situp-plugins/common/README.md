# Common SITUP plugins

## `bounded_blocking`

A buffer based off `LinkedBlockingQueue` bounded to the specified capacity. One can read and write records with specified timeout value.

- buffer_size (int): the capacity of the buffer
- batch_size (int): the maximum number of records that can be returned on read before timeout.

## `string_coverter`

A processor plugin to generate new string records with upper or lower case conversion on the content of input records.

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