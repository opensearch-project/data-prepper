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
    | arithmeticExpression EOF
    | stringExpression EOF
    | OTHER {System.err.println("unknown char: " + $OTHER.text);}
    ;

stringExpression
    : stringExpression PLUS stringExpression
    | function
    | jsonPointer
    | String
    ;

arithmeticExpression
    : arithmeticExpression (PLUS | SUBTRACT) multiplicativeExpression
    | multiplicativeExpression
    ;

multiplicativeExpression
    : multiplicativeExpression (MULTIPLY | DIVIDE) arithmeticTerm
    | arithmeticTerm
    ;

arithmeticTerm
    : function
    | jsonPointer
    | Integer
    | Float
    | LPAREN arithmeticExpression RPAREN
    | arithmeticUnaryExpression
    ;

arithmeticUnaryExpression
    : arithmeticUnaryOperator arithmeticTerm
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
    | relationalOperatorExpression relationalOperator typeOfOperatorExpression
    | setOperatorExpression
    | typeOfOperatorExpression
    ;

relationalOperator
    : LT
    | LTE
    | GT
    | GTE
    ;

typeOfOperatorExpression
    : JsonPointer TYPEOF DataTypes
    ;

setOperatorExpression
    : setOperatorExpression setOperator setInitializer
    | unaryOperatorExpression
    | arithmeticUnaryExpression
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
    : LBRACE setMembers RBRACE
    ;

setMembers
    : literal (SPACE* SET_DELIMITER SPACE* literal)*
    ;

unaryOperator
    : NOT
    ;

arithmeticUnaryOperator
    : SUBTRACT
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
    : (FunctionArg SPACE* COMMA SPACE*)* SPACE* FunctionArg
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
    : ZERO
    | NonZeroDigit Digit*
    ;

Float
    : Integer '.' ZERO* Integer
    | Integer '.' ZERO* Integer EXPONENTLETTER SUBTRACT? Integer
    ;

fragment
Digit
    : ZERO
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
    : [A-Za-z0-9_.@]
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

DataTypes
    : INTEGER
    | BOOLEAN
    | BIG_DECIMAL
    | LONG
    | MAP
    | ARRAY
    | DOUBLE
    | STRING
    ;

SET_DELIMITER
    : COMMA
    ;

DIVIDE
    : FORWARDSLASH
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
TYPEOF: SPACE 'typeof' SPACE;
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
ZERO : '0';
PLUS: '+';
MULTIPLY: '*';
DOT : '.';
EXPONENTLETTER
    : 'E'
    | 'e'
    ;

INTEGER: 'integer';
BIG_DECIMAL: 'big_decimal';
BOOLEAN: 'boolean';
LONG   : 'long';
DOUBLE : 'double';
STRING : 'string';
MAP    : 'map';
ARRAY  : 'array';

fragment
SPACE : ' ';

SKIP_SPACE : [ \t\r\n] -> skip;
