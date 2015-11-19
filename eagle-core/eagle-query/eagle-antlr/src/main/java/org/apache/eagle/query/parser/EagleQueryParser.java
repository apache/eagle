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

import org.apache.eagle.query.antlr.generated.EagleFilterLexer;
import org.apache.eagle.query.antlr.generated.EagleFilterParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EagleQueryParser {
	private static final Logger LOG = LoggerFactory.getLogger(EagleQueryParser.class);
	private String _query;
	public EagleQueryParser(String query){
		_query = query;
	}

	public ORExpression parse() throws EagleQueryParseException{
		try{
			EagleFilterLexer lexer = new EagleFilterLexer(new ANTLRInputStream(_query));
			CommonTokenStream tokens = new CommonTokenStream(lexer);
			tokens.fill();
			EagleFilterParser p = new EagleFilterParser(tokens);
			p.setErrorHandler(new EagleANTLRErrorStrategy());
			p.setBuildParseTree(true);
			EagleQueryFilterListenerImpl listener = new EagleQueryFilterListenerImpl(); 
			p.addParseListener(listener);
			EagleFilterParser.FilterContext fc = p.filter();
			if(fc.exception != null){
				LOG.error("Can not successfully parse the query:" + _query, fc.exception);
				throw fc.exception;
			}
			return listener.result();
		}catch(Exception ex){
			LOG.error("Can not successfully parse the query:", ex);
			throw new EagleQueryParseException("can not successfully parse the query:" + _query);
		}
	}
}
