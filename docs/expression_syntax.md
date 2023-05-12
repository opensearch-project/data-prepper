## Supported Operators
In order of evaluation priority. _(top to bottom, left to right)_

| Level | Operator             | Description                                           | Associativity |
|-------|----------------------|-------------------------------------------------------|---------------|
| 5     | `()`                 | Priority Expression                                   | left-to-right |
| 4     | `not`, `+`, `-`      | Unary Logical NOT<br>Unary Positive<br>Unary negative | right-to-left |
| 3     | `<`, `<=`, `>`, `>=` | Relational Operators                                  | left-to-right |
| 2     | `==`, `!=`           | Equality Operators                                    | left-to-right |
| 1     | `and`, `or`          | Conditional Expression                                | left-to-right |

## Reserved for possible future functionality
Reserved symbol set: `^`, `*`, `/`, `%`, `+`, `-`, `xor`, `=`, `+=`, `-=`, `*=`, `/=`, `%=`, `++`, `--`, `${<text>}`

## Set Initializer
Defines a set or term and/or expressions.

Examples
```
# Http Status Codes
{200, 201, 202}

# Http respone payloads
{"Created", "Accepted"}

# Handle multiple event types with different keys
{/request_payload, /request_message}
```

## Priority Expression
Identifies an expression that will be evaluated at the highest priority level. Priority expression must contain an
expression or value, empty parentheses are not supported.

Examples
```
/is_cool == (/name == "Steven")
```

## Relational Operators
Tests the relationship of two numeric values. Note, the operands must be a number or Json Pointer that will resolve to a number.

Syntax
```
<Number | Json Pointer> < <Number | Json Pointer>
<Number | Json Pointer> <= <Number | Json Pointer>
<Number | Json Pointer> > <Number | Json Pointer>
<Number | Json Pointer> >= <Number | Json Pointer>
```

Examples
```
/status_code >= 200 and /status_code < 300
```

## Equality Operators
Used to test if two value are/are not equivalent.

Syntax
```
<Any> == <Any>
<Any> != <Any>
```

Examples
```
/is_cool == true
3.14 != /status_code
{1, 2} == /event/set_property
```
Equality operators can also be used to check if Json Pointer exists or not by checking with 'null'

Syntax
```
<Json Pointer> == null
<Json Pointer> != null
null == <Json Pointer>
null != <Json Pointer>
```

Examples
```
/response == null
null != /response
```

## Conditional Expression
Used to chain together multiple expressions and/or values.

Syntax
```
<Any> and <Any>
<Any> or <Any>
not <Any>
```

Examples
```
/status_code == 200 and /message == "Hello world"
/status_code == 200 or /status_code == 202
not /status_code in {200, 202}
/response == null
/response != null
```

# Definitions
### Literal
A fundamental value that has no children.
- Float _(Supports values from 3.40282347 x 10^38 to 1.40239846 x 10^-45)_
- Integer _(Supports values from -2147483648 to 2147483647)_
- Boolean _(Supports true or false)_
- Json Pointer _(See Json Pointer section for details)_
- String _(Supports Valid Java String characters)_
- Null _(Supports null check to see if a Json Pointer is present or not)_

### Expression String
The String that will be parsed for evaluation. Expression String is the highest level of a Data Prepper Expression. Only supports one
Expression String resulting in a return value. Note, an _Expression String_ is not the same as an _Expression_.

### Statement
The highest level component of the Expression String.

### Expression
A generic component that contains a _Primary_ or an _Operator_. Expressions may contain expressions. An expressions imminent children can 
contains 0-1 _Operators_.

### Primary

- _Set_
- _Priority Expression_
- _Literal_

### Operator
Hard coded token that identifies the operation use in an _Expression_.

### Json Pointer
A Literal used to reference a value within the Event provided as context for the _Expression String_. Json Pointers are identified by a 
leading `/` containing alphanumeric character or underscores, delimited by `/`. Json Pointers can use an extended character set if wrapped 
in double quotes (`"`) using the escape character `\`. Note, Json Pointer require `~` and `/` that should be used as part of the path and 
not a delimiter to be escaped.

- `~0` representing `~`
- `~1` representing `/`

Shorthand Syntax (Regex, `\w` = `[A-Za-z_]`)
```
/\w+(/\w+)*
```

Shorthand Example
```
/Hello/World/0
```

Escaped Syntax
```
"/<Valid String Characters | Escaped Character>(/<Valid String Characters | Escaped Character>)*"
```

Escaped Example
```
# Path
# { "Hello - 'world/" : [{ "\"JsonPointer\"": true }] }
"/Hello - 'world\//0/\"JsonPointer\""
```

### Function
DataPrepper has some in-built functions that can be used as operand in an expression. For example
`length(/message) > 20`
will extract `message` field from the event and compares it's length with 20. The value of `message` field is expected to be of String type. If the field is not present in the event, null is returned and the function is not applied on it. If the field's value is not String then error is thrown.
Currently the following functions are supported
 * `length()`
   - takes one argument of JsonPointer type
   - returns the length of the value of the argument passed if it's type is string.
   For example, `length("/message")` returns 10 if the key `message` exists in the event and has the value of `"1234567890"`.
 * `hasTags()`
   - takes at least one argument
   - all arguments must be of String type
   - returns true if all arguments are present in the event's tags, returns false otherwise
   For example, if event has tags "tag1", "tag2", and "tag3", `hasTags("tag1")` or `hasTags("tag1", "tag2")` would return true and `hasTags("tag4")` and `hadTags("tag1", "tag4")` would return false.


## White Space
### Operators
White space is **optional** surrounding Relational Operators, Regex Equality Operators, Equality Operators and commas.
White space is **required** surrounding Set Initializers, Priority Expressions, Set Operators, and Conditional Expressions.

### Reference Table

| Operator             | Description              | White Space Required | ✅ Valid Examples                                               | ❌ Invalid Examples                    |
|----------------------|--------------------------|----------------------|----------------------------------------------------------------|---------------------------------------|
| `{}`                 | Set Initializer          | Yes                  | `/status in {200}`                                             | `/status in{200}`                     |
| `()`                 | Priority Expression      | Yes                  | `/a==(/b==200)`<br>`/a in ({200})`                             | `/status in({200})`                   |
| `in`, `not in`       | Set Operators            | Yes                  | `/a in {200}`<br>`/a not in {400}`                             | `/a in{200, 202}`<br>`/a not in{400}` |
| `<`, `<=`, `>`, `>=` | Relational Operators     | No                   | `/status < 300`<br>`/status>=300`                              |                                       |
| `=~`, `!~`           | Regex Equality Operators | No                   | `/msg =~ "^\w*$"`<br>`/msg=~"^\w*$"`                           |                                       |
| `==`, `!=`           | Equality Operators       | No                   | `/status == 200`<br>`/status_code==200`                        |                                       |
| `and`, `or`, `not`   | Conditional Operators    | Yes                  | `/a<300 and /b>200`                                            | `/b<300and/b>200`                     |
| `,`                  | Set Value Delimiter      | No                   | `/a in {200, 202}`<br>`/a in {200,202}`<br>`/a in {200 , 202}` | `/a in {200,}`                        |

## JsonPointers

The event data structure can be nested and have multiple levels of data. JsonPointers can be leveraged within expressions
to reference elements throughout an event.
