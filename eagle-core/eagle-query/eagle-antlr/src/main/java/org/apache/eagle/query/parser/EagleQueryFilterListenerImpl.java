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
package org.apache.eagle.query.parser;

import org.apache.eagle.query.antlr.generated.EagleFilterListener;
import org.apache.eagle.query.antlr.generated.EagleFilterParser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;

public class EagleQueryFilterListenerImpl implements EagleFilterListener{
	private final static Logger LOG = LoggerFactory.getLogger(EagleQueryFilterListenerImpl.class);
	private Stack<ORExpression> _stack = new Stack<ORExpression>();
	
	public ORExpression result(){
		return _stack.pop();
	}
	
	public void enterEquation(EagleFilterParser.EquationContext ctx){
	}
	
	public void exitEquation(EagleFilterParser.EquationContext ctx){
		TerminalNode id = ctx.ID();
		TerminalNode op = ctx.OP();
		List<TerminalNode> values = ctx.VALUE();
		TerminalNode value = values.get(0);

		if(values.size() == 2){
			// value op value
			id = values.get(0);
			value = values.get(1);
		}

		if(LOG.isDebugEnabled()) LOG.debug("ID:" + id.getText() + ", OP:" + op.getText() + ", VALUE:" + value);

		AtomicExpression kv = new AtomicExpression();
		kv.setKey(id.getText());
		kv.setOp(ComparisonOperator.locateOperator(op.getText()));

		try{
			kv.setValueType(TokenType.locate(value.getText()));
		}catch (Exception ex){
			LOG.error("Failed to locate value type for: " + value.getText() + " due to exception: " + ex.getMessage(), ex);
		}

		try{
			kv.setKeyType(TokenType.locate(id.getText()));
		}catch (Exception ex){
			LOG.error("Failed to locate id type for: " + id.getText() + " due to exception: " + ex.getMessage(), ex);
		}

//		if(id != null){
			kv.setKey(postProcessNode(id.getText(),kv.getKeyType()));
//		}

//		if(value != null){
			kv.setValue(postProcessNode(value.getText(),kv.getValueType()));
			// As to List value, it will escape in List parser but not here
			if(kv.getValueType() != TokenType.LIST) kv.setValue(StringEscapeUtils.unescapeJava(kv.getValue()));
//		}

		// push to stack
		ORExpression orExpr = new ORExpression();
		ANDExpression andExpr = new ANDExpression();
		andExpr.getAtomicExprList().add(kv);
		orExpr.getANDExprList().add(andExpr);
		_stack.push(orExpr);
	}

	private String postProcessNode(String text,TokenType type){
		int len = text.length();
		int start=0,end = len;
		if(text.startsWith("\"")) start=1;
		if(text.endsWith("\"")) end = len -1;
		text = text.substring(start, end);
		if(type == TokenType.EXP){
			Matcher matcher = TokenConstant.EXP_PATTERN.matcher(text);
			if(matcher.find()){
				text = matcher.group(1);
			}
			text = text.replace(TokenConstant.ID_PREFIX,TokenConstant.WHITE_SPACE);
		}
		return text;
	}

	public void enterCombine(EagleFilterParser.CombineContext ctx){
		
	}
	
	public void exitCombine(EagleFilterParser.CombineContext ctx){
		int numChild = ctx.getChildCount();
		if(numChild == 1){
			if(LOG.isDebugEnabled()) LOG.debug("Only one child, skip ...");
			return; // does nothing for a combine which has only one equation
		}
		
		if((ctx.LPAREN() != null) && (ctx.RPAREN() != null)){
			if(LOG.isDebugEnabled()) LOG.debug("LPAREN + RPAREN rule matched, skip ...");
			return; // does nothing for a combine which is within parenthesis
		}
		ORExpression orExprRight = _stack.pop();
		ORExpression orExprLeft = _stack.pop();
		TerminalNode node = ctx.AND();
		if(node != null){
			ORExpression newORExpr = new ORExpression();
			for(ANDExpression left : orExprLeft.getANDExprList()){
				for(ANDExpression right : orExprRight.getANDExprList()){
					ANDExpression tmp = new ANDExpression();
					tmp.getAtomicExprList().addAll(left.getAtomicExprList());
					tmp.getAtomicExprList().addAll(right.getAtomicExprList());
					newORExpr.getANDExprList().add(tmp);
				}
			}
			_stack.push(newORExpr);
			return;
		}
		
		node = ctx.OR();
		if(node != null){
			ORExpression newORExpr = new ORExpression();
			for(ANDExpression andExpr : orExprLeft.getANDExprList()){
				newORExpr.getANDExprList().add(andExpr);
			}
			for(ANDExpression andExpr : orExprRight.getANDExprList()){
				newORExpr.getANDExprList().add(andExpr);
			}
			_stack.push(newORExpr);
			return;
		}
		LOG.warn("Should never come here!");
	}

	public void enterFilter(EagleFilterParser.FilterContext ctx){
		
	}
	
	public void exitFilter(EagleFilterParser.FilterContext ctx){
		// print all relations (KeyValueFilter AND KeyValueFilter) OR (KeyValueFilter AND KeyValueFilter) OR (KeyValueFilter AND KeyValueFilter)"
		ORExpression orExpr = _stack.peek();
		if(LOG.isDebugEnabled()) LOG.debug(orExpr.toString());
	}
	
	public void visitTerminal(TerminalNode node){
		
	}
	public void visitErrorNode(ErrorNode node){

	}
    public void enterEveryRule(ParserRuleContext ctx){
    }
    
    public void exitEveryRule(ParserRuleContext ctx){
    	
    }
}
