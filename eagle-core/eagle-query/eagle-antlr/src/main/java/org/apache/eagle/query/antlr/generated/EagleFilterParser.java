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

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class EagleFilterParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.5", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		WHITESPACE=1, OP=2, AND=3, OR=4, ID=5, VALUE=6, SINGLE_VALUE=7, EXPR=8, 
		NUMBER=9, NULL=10, SET=11, DOUBLEQUOTED_STRING=12, LPAREN=13, RPAREN=14, 
		LBRACE=15, RBRACE=16;
	public static final int
		RULE_filter = 0, RULE_combine = 1, RULE_equation = 2;
	public static final String[] ruleNames = {
		"filter", "combine", "equation"
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

	@Override
	public String getGrammarFileName() { return "EagleFilter.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public EagleFilterParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class FilterContext extends ParserRuleContext {
		public CombineContext combine() {
			return getRuleContext(CombineContext.class,0);
		}
		public TerminalNode EOF() { return getToken(EagleFilterParser.EOF, 0); }
		public FilterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_filter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof EagleFilterListener ) ((EagleFilterListener)listener).enterFilter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof EagleFilterListener ) ((EagleFilterListener)listener).exitFilter(this);
		}
	}

	public final FilterContext filter() throws RecognitionException {
		FilterContext _localctx = new FilterContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_filter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(6);
			combine(0);
			setState(7);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CombineContext extends ParserRuleContext {
		public TerminalNode LPAREN() { return getToken(EagleFilterParser.LPAREN, 0); }
		public List<CombineContext> combine() {
			return getRuleContexts(CombineContext.class);
		}
		public CombineContext combine(int i) {
			return getRuleContext(CombineContext.class,i);
		}
		public TerminalNode RPAREN() { return getToken(EagleFilterParser.RPAREN, 0); }
		public EquationContext equation() {
			return getRuleContext(EquationContext.class,0);
		}
		public TerminalNode AND() { return getToken(EagleFilterParser.AND, 0); }
		public TerminalNode OR() { return getToken(EagleFilterParser.OR, 0); }
		public CombineContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_combine; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof EagleFilterListener ) ((EagleFilterListener)listener).enterCombine(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof EagleFilterListener ) ((EagleFilterListener)listener).exitCombine(this);
		}
	}

	public final CombineContext combine() throws RecognitionException {
		return combine(0);
	}

	private CombineContext combine(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		CombineContext _localctx = new CombineContext(_ctx, _parentState);
		CombineContext _prevctx = _localctx;
		int _startState = 2;
		enterRecursionRule(_localctx, 2, RULE_combine, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(15);
			switch (_input.LA(1)) {
			case LPAREN:
				{
				setState(10);
				match(LPAREN);
				setState(11);
				combine(0);
				setState(12);
				match(RPAREN);
				}
				break;
			case ID:
			case VALUE:
				{
				setState(14);
				equation();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(25);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,2,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(23);
					switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
					case 1:
						{
						_localctx = new CombineContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_combine);
						setState(17);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(18);
						match(AND);
						setState(19);
						combine(3);
						}
						break;
					case 2:
						{
						_localctx = new CombineContext(_parentctx, _parentState);
						pushNewRecursionContext(_localctx, _startState, RULE_combine);
						setState(20);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(21);
						match(OR);
						setState(22);
						combine(2);
						}
						break;
					}
					} 
				}
				setState(27);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,2,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class EquationContext extends ParserRuleContext {
		public List<TerminalNode> VALUE() { return getTokens(EagleFilterParser.VALUE); }
		public TerminalNode VALUE(int i) {
			return getToken(EagleFilterParser.VALUE, i);
		}
		public TerminalNode OP() { return getToken(EagleFilterParser.OP, 0); }
		public TerminalNode ID() { return getToken(EagleFilterParser.ID, 0); }
		public EquationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_equation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof EagleFilterListener ) ((EagleFilterListener)listener).enterEquation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof EagleFilterListener ) ((EagleFilterListener)listener).exitEquation(this);
		}
	}

	public final EquationContext equation() throws RecognitionException {
		EquationContext _localctx = new EquationContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_equation);
		try {
			setState(34);
			switch (_input.LA(1)) {
			case VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(28);
				match(VALUE);
				setState(29);
				match(OP);
				setState(30);
				match(VALUE);
				}
				break;
			case ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(31);
				match(ID);
				setState(32);
				match(OP);
				setState(33);
				match(VALUE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 1:
			return combine_sempred((CombineContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean combine_sempred(CombineContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 2);
		case 1:
			return precpred(_ctx, 1);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\22\'\4\2\t\2\4\3"+
		"\t\3\4\4\t\4\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\5\3\22\n\3\3\3\3\3\3"+
		"\3\3\3\3\3\3\3\7\3\32\n\3\f\3\16\3\35\13\3\3\4\3\4\3\4\3\4\3\4\3\4\5\4"+
		"%\n\4\3\4\2\3\4\5\2\4\6\2\2\'\2\b\3\2\2\2\4\21\3\2\2\2\6$\3\2\2\2\b\t"+
		"\5\4\3\2\t\n\7\2\2\3\n\3\3\2\2\2\13\f\b\3\1\2\f\r\7\17\2\2\r\16\5\4\3"+
		"\2\16\17\7\20\2\2\17\22\3\2\2\2\20\22\5\6\4\2\21\13\3\2\2\2\21\20\3\2"+
		"\2\2\22\33\3\2\2\2\23\24\f\4\2\2\24\25\7\5\2\2\25\32\5\4\3\5\26\27\f\3"+
		"\2\2\27\30\7\6\2\2\30\32\5\4\3\4\31\23\3\2\2\2\31\26\3\2\2\2\32\35\3\2"+
		"\2\2\33\31\3\2\2\2\33\34\3\2\2\2\34\5\3\2\2\2\35\33\3\2\2\2\36\37\7\b"+
		"\2\2\37 \7\4\2\2 %\7\b\2\2!\"\7\7\2\2\"#\7\4\2\2#%\7\b\2\2$\36\3\2\2\2"+
		"$!\3\2\2\2%\7\3\2\2\2\6\21\31\33$";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}