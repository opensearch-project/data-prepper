/*
* ANTLR grammar file for parsing Logstash configurations
*/
grammar Logstash;

@header {
    package org.opensearch.dataprepper.logstash;
}
/*
* Parser Rules
*/
config: filler plugin_section filler (filler plugin_section)* filler;

filler: (COMMENT | WS | NEWLINE)*;

plugin_section: plugin_type filler '{'
      filler (branch_or_plugin filler)*
    '}';

plugin_type: ('input' | 'filter' | 'output');

branch_or_plugin: branch | plugin;

plugin:
    name filler '{'
      filler
      attributes
      filler
    '}';

attributes:( attribute (filler attribute)*)?;

attribute: name filler '=>' filler value;

name: BAREWORD | STRING;

value: plugin | BAREWORD | STRING | NUMBER | array | hash;

branch: r_if (filler else_if)* (filler r_else)?;

r_if: 'if' filler condition filler '{' filler (branch_or_plugin filler)* '}';

else_if: 'else' filler 'if' filler condition filler '{' filler ( branch_or_plugin filler)* '}';

r_else: 'else' filler '{' filler (branch_or_plugin filler)* '}';

condition: expression (filler boolean_operator filler expression)*;

expression:
    (
        ('(' filler condition filler ')')
      | negative_expression
      | in_expression
      | not_in_expression
      | compare_expression
      | regexp_expression
      | rvalue
    );

array:
    '['
    filler
    (
      value (filler ',' filler value)*
    )?
    filler
    ']';

hash:
    '{'
      filler
      hashentries?
      filler
    '}';

hashentries: hashentry (WS hashentry)*;

hashentry: hashname filler '=>' filler value;

hashname: BAREWORD | STRING | NUMBER;

boolean_operator: ('and' | 'or' | 'xor' | 'nand');

negative_expression:
    (
        ('!' filler '(' filler condition filler ')')
      | ('!' filler selector)
    );

in_expression: rvalue filler in_operator filler rvalue;

not_in_expression: rvalue filler not_in_operator filler rvalue;

rvalue: STRING | NUMBER | selector | array | method_call | regexp;

regexp:  '/' ('\\' | ~'/' .)*? '/';

selector: selector_element+;

compare_expression: rvalue filler compare_operator filler rvalue;

regexp_expression: rvalue filler  regexp_operator filler (STRING | regexp);

selector_element: '[' ~( '[' | ']' | ',' )+ ']';

in_operator: 'in';

not_in_operator: 'not' filler 'in';

method_call:
      BAREWORD filler '(' filler
        (
          rvalue ( filler ',' filler rvalue )*
        )?
      filler ')';

compare_operator: ('==' | '!=' | '<=' | '>=' | '<' | '>') ;

regexp_operator: ('=~' | '!~');

/*
* Lexer Rules
*/

COMMENT: (WS? '#' ~('\r'|'\n')*)+;

NEWLINE: ('\r'? '\n' | '\r')+ -> skip;

WS: ( NEWLINE | ' ' | '\t')+;

fragment DIGIT: [0-9];

NUMBER: '-'? DIGIT+ ('.' DIGIT*)?;

BAREWORD: [a-zA-Z0-9_]+;

STRING: DOUBLE_QUOTED_STRING | SINGLE_QUOTED_STRING;

fragment DOUBLE_QUOTED_STRING : ('"' ( '\\"' | . )*? '"');

fragment SINGLE_QUOTED_STRING : ('\'' ('\'' | . )*? '\'');