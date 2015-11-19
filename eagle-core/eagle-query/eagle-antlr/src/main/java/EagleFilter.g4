/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
grammar EagleFilter;

@header{
// package org.apache.eagle.query.antlr.generated;
}

/*------------------------------------------------------------------
 * PARSER RULES
 *------------------------------------------------------------------*/

filter
        : combine EOF
        ;
combine 
        : LPAREN combine RPAREN
		| equation        
        | combine AND combine
        | combine OR combine
        ;
equation        
        : VALUE OP VALUE
        | ID OP VALUE     
        ;

/*------------------------------------------------------------------
 * LEXER RULES
 *------------------------------------------------------------------*/
WHITESPACE
        : ( '\t' | ' ' | '\r' | '\n'| '\u000C' )+
        -> skip
        ;
OP
        : '='   | '!='
        | '>'   | '<'
        | '>='  | '<='
        | '=~'  | '!=~'
        | 'in'  | 'IN'
        | 'not' (' ')+ 'in'
        | 'NOT' (' ')+ 'IN'
        | 'contains'
        | 'CONTAINS'
        | 'not' (' ')+ 'contains'
        | 'NOT' (' ')+ 'CONTAINS'
        | 'is'  | 'IS'
        | 'is' (' ')+ 'not'
        | 'IS' (' ')+ 'NOT'
        ;
        
AND
        : 'AND'
        | 'and'
        ;             
OR
        : 'OR'
        | 'or'
        ;    
        
ID      : '@' (~[ "=()<>])+
        ;
VALUE   : EXPR
		| SINGLE_VALUE
        | SET
        ;
SINGLE_VALUE
        : DOUBLEQUOTED_STRING
        | NUMBER
        | NULL
        ;
EXPR	: ('EXP'|'exp') LBRACE (~('}'))+ RBRACE
		;        
NUMBER
        : '-'? UNSIGN_INT ('.' UNSIGN_INT)?
        ;      
NULL
        : 'NULL'
        | 'null'
        ;
SET
        : LPAREN SINGLE_VALUE? (',' SINGLE_VALUE)* RPAREN
        ;
DOUBLEQUOTED_STRING
        : '"' STRING '"'
        | '""'
        ;

fragment UNSIGN_INT : ('0'..'9')+
                    ;
// Fully support string including escaped quotes
fragment STRING     : (~('"')| '\\"')+
                    ;
LPAREN              : '('
                    ;
RPAREN              : ')'
                    ;
LBRACE              : '{'
                    ;
RBRACE              : '}'
                    ;