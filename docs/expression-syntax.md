# Data Prepper Expression Syntax

This page describes the Data Prepper expression syntax. It is available for evaluating conditionals, especially for conditional routing of events.

An example expression is shown below.

```
/loglevel == "WARN" or /loglevel == "ERROR"
```

This expression will evaluate to true when the value for the `loglevel` key in an Event is equal to either `WARN` or `ERROR`.

## Literals

Data Prepper supports the following literals in expressions.

* Float - a floating point value in the range of 3.40282347 x 10^38 to 1.40239846 x 10^-45.
* Integer - an integer value in the range of -2147483648 to 2147483647.
* Boolean - a boolean value: `true` or `false`.
* String - a string value, enclosed in double quotes.
* Json Pointer - a JSON pointer to a key within the Event.

## Operators

Data Prepper supports the following operators. They are all standard operators as found in other languages.

* `()` - grouping
* `and` - boolean and
* `or` - boolean or
* `not`  - standard not
* `==` - equality
* `!=` - inequality
* `<` - less than
* `<=` - less than or equal
* `>` - greater than
* `>=` - greater than or equal

## More Information

For more information see [[RFC] Data Prepper Expression Syntax #1005](https://github.com/opensearch-project/data-prepper/issues/1005). Please note that not all features
described in the RFC are currently implemented.
