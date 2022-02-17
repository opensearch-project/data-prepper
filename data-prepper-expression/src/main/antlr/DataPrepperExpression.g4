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
    : 'and'
    | 'or'
    ;

equalityOperatorExpression
    : equalityOperatorExpression equalityOperator regexOperatorExpression
    | regexOperatorExpression
    ;

equalityOperator
    : '=='
    | '!='
    ;

regexOperatorExpression
    : regexPattern regexEqualityOperator regexPattern
    | relationalOperatorExpression
    ;

regexEqualityOperator
    : '=~'
    | '!~'
    ;

relationalOperatorExpression
    : relationalOperatorExpression relationalOperator setOperatorExpression
    | setOperatorExpression
    ;

relationalOperator
    : '<'
    | '<='
    | '>'
    | '>='
    ;

setOperatorExpression
    : setOperatorExpression setOperator setInitializer
    | unaryOperatorExpression
    ;

setOperator
    : 'in'
    | 'not in'
    ;

unaryOperatorExpression
    : primary
    | setInitializer
    | regexPattern
    | parenthesesExpression
    | unaryNotOperatorExpression
    ;

parenthesesExpression
    : '(' conditionalExpression ')'
    ;

regexPattern
    : jsonPointer
    | String
    ;

setInitializer
    : '{' primary (',' primary)* '}'
    ;

unaryNotOperatorExpression
    : unaryOperator conditionalExpression
    ;

unaryOperator
    : 'not'
    ;

primary
    : jsonPointer
    | variableIdentifier
    | setInitializer
    | literal
    ;

jsonPointer
    : JsonPointer
    | EscapedJsonPointer
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

EQUAL : '==';
NOT_EQUAL : '!=';
LT : '<';
GT : '>';
LTE : '<=';
GTE : '>=';
MATCH_REGEX_PATTERN : '=~';
NOT_MATCH_REGEX_PATTERN : '!~';
IN_SET : 'in';
NOT_IN_SET : 'not in';
AND : 'and';
OR : 'or';
NOT : 'not';
LPAREN : '(';
RPAREN : ')';
LBRACE : '{';
RBRACE : '}';
FORWARDSLASH : '/';
DOUBLEQUOTE : '"';
SET_SEPARATOR : ',';

SPACE
    : [ \t\r\n] -> skip
    ;
