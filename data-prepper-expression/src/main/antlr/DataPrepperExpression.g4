/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

grammar DataPrepperExpression;

@header {
    package org.opensearch.dataprepper.expression.antlr;
}

expression
    : conditionalExpression EOF
    | OTHER {System.err.println("unknown char: " + $OTHER.text);}
    ;

conditionalExpression
    : conditionalExpression conditionalOperator equalityOperatorExpression
    | equalityOperatorExpression
    ;

conditionalOperator
    : AND
    | OR
    ;

equalityOperatorExpression
    : equalityOperatorExpression equalityOperator regexOperatorExpression
    | regexOperatorExpression
    ;

equalityOperator
    : EQUAL
    | NOT_EQUAL
    ;

regexOperatorExpression
    : regexPattern regexEqualityOperator regexPattern
    | relationalOperatorExpression
    ;

regexEqualityOperator
    : MATCH_REGEX_PATTERN
    | NOT_MATCH_REGEX_PATTERN
    ;

relationalOperatorExpression
    : relationalOperatorExpression relationalOperator setOperatorExpression
    | setOperatorExpression
    ;

relationalOperator
    : LT
    | LTE
    | GT
    | GTE
    ;

setOperatorExpression
    : setOperatorExpression setOperator setInitializer
    | unaryOperatorExpression
    ;

setOperator
    : IN_SET
    | NOT_IN_SET
    ;

unaryOperatorExpression
    : primary
    | setInitializer
    | parenthesesExpression
    | unaryOperator (primary | unaryOperatorExpression)
    ;

parenthesesExpression
    : LPAREN conditionalExpression RPAREN
    ;

regexPattern
    : jsonPointer
    | String
    ;

setInitializer
    : LBRACE primary (SET_DELIMITER primary)* RBRACE
    ;

unaryOperator
    : NOT
    | SUBTRACT
    ;

primary
    : jsonPointer
    | function
    | variableIdentifier
    | setInitializer
    | literal
    ;

jsonPointer
    : JsonPointer
    | EscapedJsonPointer
    ;

function
    : Function
    ;

Function
    : JsonPointerCharacters LPAREN FunctionArgs RPAREN
    ;

fragment
FunctionArgs
    : (FunctionArg SPACE* COMMA)* SPACE* FunctionArg
    ;

fragment
FunctionArg
    : JsonPointer
    | String
    ;

variableIdentifier
    : variableName
    ;

variableName
    : VariableIdentifier
    ;

literal
    : Float
    | Integer
    | Boolean
    | String
    | Null
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

fragment
Digit
    : '0'
    | NonZeroDigit
    ;

fragment
NonZeroDigit
    : [1-9]
    ;

Boolean
    : 'true'
    | 'false'
    ;

Null
    : 'null'
    ;

JsonPointer
    : FORWARDSLASH JsonPointerCharacters (FORWARDSLASH JsonPointerCharacters)*
    ;

fragment
JsonPointerCharacters
    : JsonPointerCharacter+
    ;

fragment
JsonPointerCharacter
    : [A-Za-z0-9_]
    ;

EscapedJsonPointer
    : DOUBLEQUOTE FORWARDSLASH JsonPointerStringCharacters? DOUBLEQUOTE
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
JsonPointerEscapeSequence
    : '\\' [btnfr"'\\/]
    ;

VariableIdentifier
    : '${' VariableNameCharacters '}'
    ;

fragment
VariableNameCharacters
    : VariableNameLeadingCharacter VariableNameCharacter*
    ;

fragment
VariableNameLeadingCharacter
    : [A-Za-z_]
    ;

fragment
VariableNameCharacter
    : VariableNameLeadingCharacter
    | [0-9-]
    ;

String
    : DOUBLEQUOTE StringCharacters? DOUBLEQUOTE
    ;

fragment
StringCharacters
    : StringCharacter+
    ;

fragment
StringCharacter
    : ~["\\$]
    | EscapeSequence
    ;

fragment
EscapeSequence
    : '\\' [btnfr"'\\$]
    ;

SET_DELIMITER
    : COMMA
    ;
COMMA : ',';
EQUAL : '==';
NOT_EQUAL : '!=';
LT : '<';
GT : '>';
LTE : '<=';
GTE : '>=';
MATCH_REGEX_PATTERN : '=~';
NOT_MATCH_REGEX_PATTERN : '!~';
IN_SET : SPACE 'in' SPACE;
NOT_IN_SET : SPACE 'not in' SPACE;
AND : SPACE 'and' SPACE;
OR : SPACE 'or' SPACE;
NOT : 'not' SPACE;
SUBTRACT : '-';
LPAREN : '(';
RPAREN : ')';
LBRACE : '{';
RBRACE : '}';
FORWARDSLASH : '/';
DOUBLEQUOTE : '"';

fragment
SPACE : ' ';

SKIP_SPACE : [ \t\r\n] -> skip;
