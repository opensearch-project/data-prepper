/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

grammar DataPrepperScript;

@header {
    package org.opensearch.dataprepper.script.antlr;
}

fragment Digit
    : [0-9]
    ;

fragment NumberCharacters
    : Digit
    | '.'
    ;

Number
    : Digit (NumberCharacters)*
    ;

Boolean
    : ( TRUE | FALSE )
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
    : [A-Za-z0-9]
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

expr
    : term
    | list
    | group
    | NOT expr
    | expr op=(IN | NOTIN) expr
    | expr op=(LT | LTEQ | GT | GTEQ) expr
    | expr op=(REGEQ | REGNEQ) expr
    | expr op=(EQ | NEQ) expr
    | expr op=(AND | OR) expr
    | OTHER {System.err.println("unknown char: " + $OTHER.text);}
    ;

group
    : LPAREN expr RPAREN
    ;

listItems
    : expr (LISTSEPARATOR expr)*
    ;

list
    : LBOXBRACKET listItems? RBOXBRACKET
    ;

term
    : Number
    | Boolean
    | JsonPointer
    | String
    ;

EQ : '==';
NEQ : '!=';
LT : '<';
LTEQ : '<=';
GT : '>';
GTEQ : '>=';
REGEQ : '=~';
REGNEQ : '!~';
IN : 'in';
NOTIN : 'not in';
AND : 'and';
OR : 'or';
NOT : 'not';
LPAREN : '(';
RPAREN : ')';
LBOXBRACKET : '[';
RBOXBRACKET : ']';
TRUE : 'true';
FALSE : 'false';
FORWARDSLASH : '/';
DOUBLEQUOTE : '"';
LISTSEPARATOR : ',';

SPACE
    : [ \t\r\n] -> skip
    ;
