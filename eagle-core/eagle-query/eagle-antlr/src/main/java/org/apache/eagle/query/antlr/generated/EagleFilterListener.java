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

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link EagleFilterParser}.
 */
public interface EagleFilterListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link EagleFilterParser#filter}.
	 * @param ctx the parse tree
	 */
	void enterFilter(EagleFilterParser.FilterContext ctx);
	/**
	 * Exit a parse tree produced by {@link EagleFilterParser#filter}.
	 * @param ctx the parse tree
	 */
	void exitFilter(EagleFilterParser.FilterContext ctx);
	/**
	 * Enter a parse tree produced by {@link EagleFilterParser#combine}.
	 * @param ctx the parse tree
	 */
	void enterCombine(EagleFilterParser.CombineContext ctx);
	/**
	 * Exit a parse tree produced by {@link EagleFilterParser#combine}.
	 * @param ctx the parse tree
	 */
	void exitCombine(EagleFilterParser.CombineContext ctx);
	/**
	 * Enter a parse tree produced by {@link EagleFilterParser#equation}.
	 * @param ctx the parse tree
	 */
	void enterEquation(EagleFilterParser.EquationContext ctx);
	/**
	 * Exit a parse tree produced by {@link EagleFilterParser#equation}.
	 * @param ctx the parse tree
	 */
	void exitEquation(EagleFilterParser.EquationContext ctx);
}