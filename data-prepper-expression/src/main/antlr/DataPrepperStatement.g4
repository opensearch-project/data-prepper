/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

grammar DataPrepperStatement;

@header {
    package org.opensearch.dataprepper.expression.antlr;
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

fragment
VariableNameLeadingCharacter
    : [A-Za-z_]
    ;

fragment
VariableNameCharacter
    : VariableNameLeadingCharacter
    | [0-9-]
    ;

fragment
VariableNameCharacters
    : VariableNameLeadingCharacter VariableNameCharacter*
    ;

VariableIdentifier
    : '${' VariableNameCharacters '}'
    ;

statement
    : expression EOF
    | OTHER {System.err.println("unknown char: " + $OTHER.text);}
    ;

expression
    : binaryOperatorExpression
    ;

binaryOperatorExpression
    : conditionalExpression
    ;

conditionalExpression
    : conditionalExpression conditionalOperator equalityOperatorExpression
    | equalityOperatorExpression
    ;

equalityOperatorExpression
    : equalityOperatorExpression binaryOperator regexOperatorExpression
    | regexOperatorExpression
    ;

regexOperatorExpression
    : regexOperatorExpression regexEqualityOperator regexPattern
    | relationalOperatorExpression
    ;

relationalOperatorExpression
    : relationalOperatorExpression relationalOperator listOperatorExpression
    | listOperatorExpression
    ;

listOperatorExpression
    : listOperatorExpression listOperator listInitializer
    | unaryOperatorExpression
    ;

unaryOperatorExpression
    : primary
    | listInitializer
    | regexPattern
    | unaryNotOperatorExpression
    ;

unaryNotOperatorExpression
    : unaryOperator primary
    ;

binaryOperator
    : relationalOperator
    | equalityOperator
    ;

regexEqualityOperator
    : '=~'
    | '!~'
    ;

listOperator
    : 'in'
    | 'not in'
    ;


conditionalOperator
    : 'and'
    | 'or'
    ;

unaryOperator
    : 'not'
    ;

equalityOperator
    : '=='
    | '!='
    ;

relationalOperator
    : '<'
    | '<='
    | '>'
    | '>='
    ;

primary
    : literal
    | variableIdentifier
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
    : '{' binaryOperatorExpression (',' binaryOperatorExpression)* '}'
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
LBRACE : '{';
RBRACE : '}';
TRUE : 'true';
FALSE : 'false';
FORWARDSLASH : '/';
DOUBLEQUOTE : '"';
LISTSEPARATOR : ',';
PERIOD : '.';

SPACE
    : [ \t\r\n] -> skip
    ;
