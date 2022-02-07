/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

grammar DataPrepperScript;

@header {
    package org.opensearch.dataprepper.script.antlr;
}

fragment
Digit
    : '0'
    | NonZeroDigit
    ;

fragment
NonZeroDigit
    : [1-9]
    ;

Integer
    : '0'
    | NonZeroDigit Digit*
    ;

Float
    : NonZeroDigit? Digit '.' Digit
    | NonZeroDigit? Digit '.' Digit+ NonZeroDigit
    | '.' Digit
    | '.' Digit* NonZeroDigit
    ;

Boolean
    : TRUE
    | FALSE
    ;

fragment
StringCharacters
    : StringCharacter+
    ;

fragment
StringCharacter
    : ~["\\]
    | EscapeSequence
    ;

fragment
EscapeSequence
    : '\\' [btnfr"'\\]
    ;

fragment
JsonPointerEscapeSequence
    : '\\' [btnfr"'\\/]
    ;

fragment
JsonPointerStringCharacters
    : JsonPointerStringCharacter+
    ;

fragment
JsonPointerStringCharacter
    : ~["\\]
    | JsonPointerEscapeSequence
    ;

fragment
JsonPointerCharacter
    : [A-Za-z0-9_]
    ;

fragment
JsonPointerCharacters
    : JsonPointerCharacter+
    ;

JsonPointer
    : DOUBLEQUOTE FORWARDSLASH JsonPointerStringCharacters? DOUBLEQUOTE
    | FORWARDSLASH JsonPointerCharacters? (FORWARDSLASH JsonPointerCharacters)*
    ;

String
    : DOUBLEQUOTE StringCharacters? DOUBLEQUOTE
    ;

statement
    : expression EOF
    ;

expression
    : primary
    | 'not' expression
    | expression op=('in' | 'not in') listInitializer
    | expression op=('<' | '<=' | '>' | '>=') expression
    | expression op=('=~' | '!~') regexPattern
    | expression op=('==' | '!=') expression
    | expression op=('and' | 'or') expression
    | OTHER {System.err.println("unknown char: " + $OTHER.text);}
    ;

primary
    : literal
    | listInitializer
    | expressionInitializer
    ;

regexPattern
    : JsonPointer
    | String
    | '(' (JsonPointer | String) ')'
    ;

expressionInitializer
    : '(' expression ')'
    ;

listInitializer
    : '[' expression (',' expression)* ']'
    ;

literal
    : Float
    | Integer
    | Boolean
    | JsonPointer
    | String
    ;

EQUAL : '==';
NOT_EQUAL : '!=';
LT : '<';
GT : '>';
LTE : '<=';
GTE : '>=';
MATCH_REGEX_PATTERN : '=~';
NOT_MATCH_REGEX_PATTERN : '!~';
IN_LIST : 'in';
NOT_IN_LIST : 'not in';
AND : 'and';
OR : 'or';
NOT : 'not';
LPAREN : '(';
RPAREN : ')';
LBRACK : '[';
RBRACK : ']';
TRUE : 'true';
FALSE : 'false';
FORWARDSLASH : '/';
DOUBLEQUOTE : '"';
LISTSEPARATOR : ',';
PERIOD : '.';

SPACE
    : [ \t\r\n] -> skip
    ;
