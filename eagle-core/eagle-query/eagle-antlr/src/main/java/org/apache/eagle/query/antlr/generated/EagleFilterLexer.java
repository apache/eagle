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
// Generated from EagleFilter.g4 by ANTLR 4.5
package org.apache.eagle.query.antlr.generated;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class EagleFilterLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.5", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		WHITESPACE=1, OP=2, AND=3, OR=4, ID=5, VALUE=6, SINGLE_VALUE=7, EXPR=8, 
		NUMBER=9, NULL=10, SET=11, DOUBLEQUOTED_STRING=12, LPAREN=13, RPAREN=14, 
		LBRACE=15, RBRACE=16;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"WHITESPACE", "OP", "AND", "OR", "ID", "VALUE", "SINGLE_VALUE", "EXPR", 
		"NUMBER", "NULL", "SET", "DOUBLEQUOTED_STRING", "UNSIGN_INT", "STRING", 
		"LPAREN", "RPAREN", "LBRACE", "RBRACE"
	};

	private static final String[] _LITERAL_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, "'('", "')'", "'{'", "'}'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "WHITESPACE", "OP", "AND", "OR", "ID", "VALUE", "SINGLE_VALUE", 
		"EXPR", "NUMBER", "NULL", "SET", "DOUBLEQUOTED_STRING", "LPAREN", "RPAREN", 
		"LBRACE", "RBRACE"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public EagleFilterLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "EagleFilter.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\22\u010c\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\3\2\6\2)\n\2\r\2\16\2*\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\6\3E"+
		"\n\3\r\3\16\3F\3\3\3\3\3\3\3\3\3\3\3\3\3\3\6\3P\n\3\r\3\16\3Q\3\3\3\3"+
		"\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\3\3\3\3\3\3\6\3k\n\3\r\3\16\3l\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\3\3\3\3\3\3\3\6\3|\n\3\r\3\16\3}\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\6\3\u0090\n\3\r\3\16\3\u0091\3\3\3\3\3"+
		"\3\3\3\3\3\3\3\3\3\6\3\u009b\n\3\r\3\16\3\u009c\3\3\3\3\3\3\5\3\u00a2"+
		"\n\3\3\4\3\4\3\4\3\4\3\4\3\4\5\4\u00aa\n\4\3\5\3\5\3\5\3\5\5\5\u00b0\n"+
		"\5\3\6\3\6\6\6\u00b4\n\6\r\6\16\6\u00b5\3\7\3\7\3\7\5\7\u00bb\n\7\3\b"+
		"\3\b\3\b\5\b\u00c0\n\b\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u00c8\n\t\3\t\3\t\6"+
		"\t\u00cc\n\t\r\t\16\t\u00cd\3\t\3\t\3\n\5\n\u00d3\n\n\3\n\3\n\3\n\5\n"+
		"\u00d8\n\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\5\13\u00e2\n\13\3\f"+
		"\3\f\5\f\u00e6\n\f\3\f\3\f\7\f\u00ea\n\f\f\f\16\f\u00ed\13\f\3\f\3\f\3"+
		"\r\3\r\3\r\3\r\3\r\3\r\5\r\u00f7\n\r\3\16\6\16\u00fa\n\16\r\16\16\16\u00fb"+
		"\3\17\3\17\3\17\6\17\u0101\n\17\r\17\16\17\u0102\3\20\3\20\3\21\3\21\3"+
		"\22\3\22\3\23\3\23\2\2\24\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25"+
		"\f\27\r\31\16\33\2\35\2\37\17!\20#\21%\22\3\2\7\5\2\13\f\16\17\"\"\4\2"+
		">>@@\6\2\"\"$$*+>@\3\2\177\177\3\2$$\u0134\2\3\3\2\2\2\2\5\3\2\2\2\2\7"+
		"\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2"+
		"\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\37\3\2\2\2\2"+
		"!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\3(\3\2\2\2\5\u00a1\3\2\2\2\7\u00a9\3\2"+
		"\2\2\t\u00af\3\2\2\2\13\u00b1\3\2\2\2\r\u00ba\3\2\2\2\17\u00bf\3\2\2\2"+
		"\21\u00c7\3\2\2\2\23\u00d2\3\2\2\2\25\u00e1\3\2\2\2\27\u00e3\3\2\2\2\31"+
		"\u00f6\3\2\2\2\33\u00f9\3\2\2\2\35\u0100\3\2\2\2\37\u0104\3\2\2\2!\u0106"+
		"\3\2\2\2#\u0108\3\2\2\2%\u010a\3\2\2\2\')\t\2\2\2(\'\3\2\2\2)*\3\2\2\2"+
		"*(\3\2\2\2*+\3\2\2\2+,\3\2\2\2,-\b\2\2\2-\4\3\2\2\2.\u00a2\7?\2\2/\60"+
		"\7#\2\2\60\u00a2\7?\2\2\61\u00a2\t\3\2\2\62\63\7@\2\2\63\u00a2\7?\2\2"+
		"\64\65\7>\2\2\65\u00a2\7?\2\2\66\67\7?\2\2\67\u00a2\7\u0080\2\289\7#\2"+
		"\29:\7?\2\2:\u00a2\7\u0080\2\2;<\7k\2\2<\u00a2\7p\2\2=>\7K\2\2>\u00a2"+
		"\7P\2\2?@\7p\2\2@A\7q\2\2AB\7v\2\2BD\3\2\2\2CE\7\"\2\2DC\3\2\2\2EF\3\2"+
		"\2\2FD\3\2\2\2FG\3\2\2\2GH\3\2\2\2HI\7k\2\2I\u00a2\7p\2\2JK\7P\2\2KL\7"+
		"Q\2\2LM\7V\2\2MO\3\2\2\2NP\7\"\2\2ON\3\2\2\2PQ\3\2\2\2QO\3\2\2\2QR\3\2"+
		"\2\2RS\3\2\2\2ST\7K\2\2T\u00a2\7P\2\2UV\7e\2\2VW\7q\2\2WX\7p\2\2XY\7v"+
		"\2\2YZ\7c\2\2Z[\7k\2\2[\\\7p\2\2\\\u00a2\7u\2\2]^\7E\2\2^_\7Q\2\2_`\7"+
		"P\2\2`a\7V\2\2ab\7C\2\2bc\7K\2\2cd\7P\2\2d\u00a2\7U\2\2ef\7p\2\2fg\7q"+
		"\2\2gh\7v\2\2hj\3\2\2\2ik\7\"\2\2ji\3\2\2\2kl\3\2\2\2lj\3\2\2\2lm\3\2"+
		"\2\2mn\3\2\2\2no\7e\2\2op\7q\2\2pq\7p\2\2qr\7v\2\2rs\7c\2\2st\7k\2\2t"+
		"u\7p\2\2u\u00a2\7u\2\2vw\7P\2\2wx\7Q\2\2xy\7V\2\2y{\3\2\2\2z|\7\"\2\2"+
		"{z\3\2\2\2|}\3\2\2\2}{\3\2\2\2}~\3\2\2\2~\177\3\2\2\2\177\u0080\7E\2\2"+
		"\u0080\u0081\7Q\2\2\u0081\u0082\7P\2\2\u0082\u0083\7V\2\2\u0083\u0084"+
		"\7C\2\2\u0084\u0085\7K\2\2\u0085\u0086\7P\2\2\u0086\u00a2\7U\2\2\u0087"+
		"\u0088\7k\2\2\u0088\u00a2\7u\2\2\u0089\u008a\7K\2\2\u008a\u00a2\7U\2\2"+
		"\u008b\u008c\7k\2\2\u008c\u008d\7u\2\2\u008d\u008f\3\2\2\2\u008e\u0090"+
		"\7\"\2\2\u008f\u008e\3\2\2\2\u0090\u0091\3\2\2\2\u0091\u008f\3\2\2\2\u0091"+
		"\u0092\3\2\2\2\u0092\u0093\3\2\2\2\u0093\u0094\7p\2\2\u0094\u0095\7q\2"+
		"\2\u0095\u00a2\7v\2\2\u0096\u0097\7K\2\2\u0097\u0098\7U\2\2\u0098\u009a"+
		"\3\2\2\2\u0099\u009b\7\"\2\2\u009a\u0099\3\2\2\2\u009b\u009c\3\2\2\2\u009c"+
		"\u009a\3\2\2\2\u009c\u009d\3\2\2\2\u009d\u009e\3\2\2\2\u009e\u009f\7P"+
		"\2\2\u009f\u00a0\7Q\2\2\u00a0\u00a2\7V\2\2\u00a1.\3\2\2\2\u00a1/\3\2\2"+
		"\2\u00a1\61\3\2\2\2\u00a1\62\3\2\2\2\u00a1\64\3\2\2\2\u00a1\66\3\2\2\2"+
		"\u00a18\3\2\2\2\u00a1;\3\2\2\2\u00a1=\3\2\2\2\u00a1?\3\2\2\2\u00a1J\3"+
		"\2\2\2\u00a1U\3\2\2\2\u00a1]\3\2\2\2\u00a1e\3\2\2\2\u00a1v\3\2\2\2\u00a1"+
		"\u0087\3\2\2\2\u00a1\u0089\3\2\2\2\u00a1\u008b\3\2\2\2\u00a1\u0096\3\2"+
		"\2\2\u00a2\6\3\2\2\2\u00a3\u00a4\7C\2\2\u00a4\u00a5\7P\2\2\u00a5\u00aa"+
		"\7F\2\2\u00a6\u00a7\7c\2\2\u00a7\u00a8\7p\2\2\u00a8\u00aa\7f\2\2\u00a9"+
		"\u00a3\3\2\2\2\u00a9\u00a6\3\2\2\2\u00aa\b\3\2\2\2\u00ab\u00ac\7Q\2\2"+
		"\u00ac\u00b0\7T\2\2\u00ad\u00ae\7q\2\2\u00ae\u00b0\7t\2\2\u00af\u00ab"+
		"\3\2\2\2\u00af\u00ad\3\2\2\2\u00b0\n\3\2\2\2\u00b1\u00b3\7B\2\2\u00b2"+
		"\u00b4\n\4\2\2\u00b3\u00b2\3\2\2\2\u00b4\u00b5\3\2\2\2\u00b5\u00b3\3\2"+
		"\2\2\u00b5\u00b6\3\2\2\2\u00b6\f\3\2\2\2\u00b7\u00bb\5\21\t\2\u00b8\u00bb"+
		"\5\17\b\2\u00b9\u00bb\5\27\f\2\u00ba\u00b7\3\2\2\2\u00ba\u00b8\3\2\2\2"+
		"\u00ba\u00b9\3\2\2\2\u00bb\16\3\2\2\2\u00bc\u00c0\5\31\r\2\u00bd\u00c0"+
		"\5\23\n\2\u00be\u00c0\5\25\13\2\u00bf\u00bc\3\2\2\2\u00bf\u00bd\3\2\2"+
		"\2\u00bf\u00be\3\2\2\2\u00c0\20\3\2\2\2\u00c1\u00c2\7G\2\2\u00c2\u00c3"+
		"\7Z\2\2\u00c3\u00c8\7R\2\2\u00c4\u00c5\7g\2\2\u00c5\u00c6\7z\2\2\u00c6"+
		"\u00c8\7r\2\2\u00c7\u00c1\3\2\2\2\u00c7\u00c4\3\2\2\2\u00c8\u00c9\3\2"+
		"\2\2\u00c9\u00cb\5#\22\2\u00ca\u00cc\n\5\2\2\u00cb\u00ca\3\2\2\2\u00cc"+
		"\u00cd\3\2\2\2\u00cd\u00cb\3\2\2\2\u00cd\u00ce\3\2\2\2\u00ce\u00cf\3\2"+
		"\2\2\u00cf\u00d0\5%\23\2\u00d0\22\3\2\2\2\u00d1\u00d3\7/\2\2\u00d2\u00d1"+
		"\3\2\2\2\u00d2\u00d3\3\2\2\2\u00d3\u00d4\3\2\2\2\u00d4\u00d7\5\33\16\2"+
		"\u00d5\u00d6\7\60\2\2\u00d6\u00d8\5\33\16\2\u00d7\u00d5\3\2\2\2\u00d7"+
		"\u00d8\3\2\2\2\u00d8\24\3\2\2\2\u00d9\u00da\7P\2\2\u00da\u00db\7W\2\2"+
		"\u00db\u00dc\7N\2\2\u00dc\u00e2\7N\2\2\u00dd\u00de\7p\2\2\u00de\u00df"+
		"\7w\2\2\u00df\u00e0\7n\2\2\u00e0\u00e2\7n\2\2\u00e1\u00d9\3\2\2\2\u00e1"+
		"\u00dd\3\2\2\2\u00e2\26\3\2\2\2\u00e3\u00e5\5\37\20\2\u00e4\u00e6\5\17"+
		"\b\2\u00e5\u00e4\3\2\2\2\u00e5\u00e6\3\2\2\2\u00e6\u00eb\3\2\2\2\u00e7"+
		"\u00e8\7.\2\2\u00e8\u00ea\5\17\b\2\u00e9\u00e7\3\2\2\2\u00ea\u00ed\3\2"+
		"\2\2\u00eb\u00e9\3\2\2\2\u00eb\u00ec\3\2\2\2\u00ec\u00ee\3\2\2\2\u00ed"+
		"\u00eb\3\2\2\2\u00ee\u00ef\5!\21\2\u00ef\30\3\2\2\2\u00f0\u00f1\7$\2\2"+
		"\u00f1\u00f2\5\35\17\2\u00f2\u00f3\7$\2\2\u00f3\u00f7\3\2\2\2\u00f4\u00f5"+
		"\7$\2\2\u00f5\u00f7\7$\2\2\u00f6\u00f0\3\2\2\2\u00f6\u00f4\3\2\2\2\u00f7"+
		"\32\3\2\2\2\u00f8\u00fa\4\62;\2\u00f9\u00f8\3\2\2\2\u00fa\u00fb\3\2\2"+
		"\2\u00fb\u00f9\3\2\2\2\u00fb\u00fc\3\2\2\2\u00fc\34\3\2\2\2\u00fd\u0101"+
		"\n\6\2\2\u00fe\u00ff\7^\2\2\u00ff\u0101\7$\2\2\u0100\u00fd\3\2\2\2\u0100"+
		"\u00fe\3\2\2\2\u0101\u0102\3\2\2\2\u0102\u0100\3\2\2\2\u0102\u0103\3\2"+
		"\2\2\u0103\36\3\2\2\2\u0104\u0105\7*\2\2\u0105 \3\2\2\2\u0106\u0107\7"+
		"+\2\2\u0107\"\3\2\2\2\u0108\u0109\7}\2\2\u0109$\3\2\2\2\u010a\u010b\7"+
		"\177\2\2\u010b&\3\2\2\2\33\2*FQl}\u0091\u009c\u00a1\u00a9\u00af\u00b5"+
		"\u00ba\u00bf\u00c7\u00cd\u00d2\u00d7\u00e1\u00e5\u00eb\u00f6\u00fb\u0100"+
		"\u0102\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}