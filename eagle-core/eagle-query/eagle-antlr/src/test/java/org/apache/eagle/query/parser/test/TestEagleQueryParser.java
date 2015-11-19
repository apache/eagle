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
package org.apache.eagle.query.parser.test;

import junit.framework.Assert;
import org.apache.eagle.query.parser.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestEagleQueryParser {
	
	private static final Logger LOG = LoggerFactory.getLogger(TestEagleQueryParser.class);
	
	@Test
	public void testSingleExpression(){
		String query = "@cluster=\"a\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@cluster", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("a", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("=", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		LOG.info(or.toString());
		Assert.assertEquals("(@cluster=\"a\")", or.toString());
	}

	@Test
	public void testLessThanExpression(){
		String query = "@field1<\"1\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@field1", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("1", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("<", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		LOG.info(or.toString());
		Assert.assertEquals("(@field1<\"1\")", or.toString());
	}

	@Test
	public void testLessOrEqualExpression(){
		String query = "@field1<=\"1\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@field1", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("1", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("<=", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		LOG.info(or.toString());
		Assert.assertEquals("(@field1<=\"1\")", or.toString());
	}

	@Test
	public void testGreaterThanExpression(){
		String query = "@field1>\"1\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@field1", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("1", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals(">", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		LOG.info(or.toString());
		Assert.assertEquals("(@field1>\"1\")", or.toString());
	}

	@Test
	public void testGreaterOrEqualExpression(){
		String query = "@field1>=\"1\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@field1", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("1", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals(">=", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.STRING, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		LOG.info(or.toString());
		Assert.assertEquals("(@field1>=\"1\")", or.toString());
	}

	@Test
	public void testMultipleANDExpression(){
		String query = "@cluster=\"abc\" AND @host=\"dc123.xyz.com\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(2, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@cluster", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("abc", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals(TokenType.STRING, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());
		Assert.assertEquals("=", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals("@host", or.getANDExprList().get(0).getAtomicExprList().get(1).getKey());
		Assert.assertEquals("dc123.xyz.com", or.getANDExprList().get(0).getAtomicExprList().get(1).getValue());
		Assert.assertEquals(TokenType.STRING, or.getANDExprList().get(0).getAtomicExprList().get(1).getValueType());
		Assert.assertEquals("=", or.getANDExprList().get(0).getAtomicExprList().get(1).getOp().toString());
		LOG.info(or.toString());
		Assert.assertEquals("(@cluster=\"abc\" AND @host=\"dc123.xyz.com\")", or.toString());
		
		query = "@datacenter=\"dc1\" AND @cluster=\"abc\" AND @host=\"dc123.xyz.com\" ";
		parser = new EagleQueryParser(query);
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(3, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@datacenter", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("dc1", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("=", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals("@cluster", or.getANDExprList().get(0).getAtomicExprList().get(1).getKey());
		Assert.assertEquals("abc", or.getANDExprList().get(0).getAtomicExprList().get(1).getValue());
		Assert.assertEquals("=", or.getANDExprList().get(0).getAtomicExprList().get(1).getOp().toString());
		Assert.assertEquals("@host", or.getANDExprList().get(0).getAtomicExprList().get(2).getKey());
		Assert.assertEquals("dc123.xyz.com", or.getANDExprList().get(0).getAtomicExprList().get(2).getValue());
		Assert.assertEquals("=", or.getANDExprList().get(0).getAtomicExprList().get(2).getOp().toString());
		LOG.info(or.toString());
		Assert.assertEquals("(@datacenter=\"dc1\" AND @cluster=\"abc\" AND @host=\"dc123.xyz.com\")", or.toString());
	}
	
	@Test
	public void testMultipleORExpression(){
		String query = "@cluster=\"abc\" OR @host=\"dc123.xyz.com\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==2);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals(1, or.getANDExprList().get(1).getAtomicExprList().size());
		Assert.assertEquals("@cluster", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("abc", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("=", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals("@host", or.getANDExprList().get(1).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("dc123.xyz.com", or.getANDExprList().get(1).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("=", or.getANDExprList().get(1).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals("(@cluster=\"abc\") OR (@host=\"dc123.xyz.com\")", or.toString());
		
		query = "@datacenter=\"dc1\" OR @cluster=\"abc\" OR @host=\"dc123.xyz.com\"";
		parser = new EagleQueryParser(query);
		or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}		Assert.assertTrue(or.getANDExprList().size()==3);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals(1, or.getANDExprList().get(1).getAtomicExprList().size());
		Assert.assertEquals(1, or.getANDExprList().get(2).getAtomicExprList().size());
		Assert.assertEquals("@datacenter", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("dc1", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("=", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals("@cluster", or.getANDExprList().get(1).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("abc", or.getANDExprList().get(1).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("=", or.getANDExprList().get(1).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals("@host", or.getANDExprList().get(2).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("dc123.xyz.com", or.getANDExprList().get(2).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("=", or.getANDExprList().get(2).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals("(@datacenter=\"dc1\") OR (@cluster=\"abc\") OR (@host=\"dc123.xyz.com\")", or.toString());
	}
	
	@Test
	public void testANDORCombination(){
		String query = "@cluster=\"abc\" OR @host=\"dc123.xyz.com\" AND @datacenter=\"dc1\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}		LOG.info(or.toString());
		Assert.assertEquals("(@cluster=\"abc\") OR (@host=\"dc123.xyz.com\" AND @datacenter=\"dc1\")", or.toString());
		
		query = "(@cluster=\"abc\" AND @host=\"dc123.xyz.com\") AND @datacenter=\"dc1\"";
		parser = new EagleQueryParser(query);
		or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}		LOG.info(or.toString());
		Assert.assertEquals("(@cluster=\"abc\" AND @host=\"dc123.xyz.com\" AND @datacenter=\"dc1\")", or.toString());

		query = "(@cluster=\"abc\" OR @host=\"dc123.xyz.com\") AND @datacenter=\"dc1\"";
		parser = new EagleQueryParser(query);
		or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		LOG.info(or.toString());
		Assert.assertEquals("(@cluster=\"abc\" AND @datacenter=\"dc1\") OR (@host=\"dc123.xyz.com\" AND @datacenter=\"dc1\")", or.toString());
		
		query = "(@cluster=\"abc\" OR @host=\"dc123.xyz.com\") AND (@datacenter=\"dc1\" OR @cluster=\"bcd\")";
		parser = new EagleQueryParser(query);
		or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		LOG.info(or.toString());
		Assert.assertEquals("(@cluster=\"abc\" AND @datacenter=\"dc1\") OR (@cluster=\"abc\" AND @cluster=\"bcd\") OR (@host=\"dc123.xyz.com\" AND @datacenter=\"dc1\") OR (@host=\"dc123.xyz.com\" AND @cluster=\"bcd\")", or.toString());
		
		query = "(@cluster=\"abc\" OR @host=\"dc123.xyz.com\") AND (@datacenter=\"dc1\" AND @cluster=\"bcd\")";
		parser = new EagleQueryParser(query);
		or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		LOG.info(or.toString());
		Assert.assertEquals("(@cluster=\"abc\" AND @datacenter=\"dc1\" AND @cluster=\"bcd\") OR (@host=\"dc123.xyz.com\" AND @datacenter=\"dc1\" AND @cluster=\"bcd\")", or.toString());
	}
	
	@Test
	public void testNegativeCase(){
		String query = "@cluster      = \"a\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		LOG.info(or.toString());
		Assert.assertEquals("(@cluster=\"a\")", or.toString());
		
		query = "@cluster    = a\"";
		parser = new EagleQueryParser(query);
		or = null;
		try{
			or = parser.parse();
		}catch(Exception ex){
			LOG.error("Can not successfully parse the query:" + query, ex);
		}
		Assert.assertTrue(or == null);
		
		query = "@cluster    = \"\"a\"";
		parser = new EagleQueryParser(query);
		or = null;
		try{
			or = parser.parse();
		}catch(Exception ex){
			LOG.error("Can not successfully parse the query:" + query, ex);
		}
		Assert.assertNotNull(or);

		query = "@cluster=\"cluster1\" AND @datacenter=\"dc1\" AND @remediationID=8888\" AND @remediationStatus=\"status\"";
		parser = new EagleQueryParser(query);
		or = null;
		try{
			or = parser.parse();
		}catch(Exception ex){
			LOG.error("Can not successfully parse the query:" + query, ex);
		}
		Assert.assertTrue(or == null);
	}
	
	@Test
	public void testSimpleWildcardMatchQuery(){
		String expected = "-[]/{}()*+?.\\^$|";
		String query = "@user=\"-[]/{}()*+?.\\\\^$|\"";
		System.out.println(query);
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@user", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals(expected, or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("=", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		LOG.info(or.toString());
		Assert.assertEquals("(@user=\""+expected+"\")", or.toString());
	}

	@Test
	public void testNumberQuery() {
		String query = "@field1 >= -1.234";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@field1", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals(-1.234, Double.parseDouble(or.getANDExprList().get(0).getAtomicExprList().get(0).getValue()));
		Assert.assertEquals(">=", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.NUMBER, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());
	}

	@Test
	public void testContainQuery() {
		String query = "@name contains \"jame\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("jame", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("CONTAINS", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.STRING, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());
	}

	@Test
	public void testNotContainQuery() {
		String query = "@name not contains \"jame\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("jame", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("NOT CONTAINS", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.STRING, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		query = "@name NOT CONTAINS \"jame\"";
		parser = new EagleQueryParser(query);
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("jame", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("NOT CONTAINS", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.STRING, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		query = "@name NOT                                          CONTAINS \"jame\"";
		parser = new EagleQueryParser(query);
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("jame", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("NOT CONTAINS", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.STRING, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());
	}

	@Test
	public void testNullQuery() {
		String query = "@name is null";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("null", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("IS", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.NULL, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		query = "@name IS               NULL";
		parser = new EagleQueryParser(query);
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("NULL", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("IS", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.NULL, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		query = "@name is not null";
		parser = new EagleQueryParser(query);
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("null", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("IS NOT", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.NULL, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		query = "@name is               not               NULL";
		parser = new EagleQueryParser(query);
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("NULL", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("IS NOT", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.NULL, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		query = "@name =               NULL";
		parser = new EagleQueryParser(query);
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("NULL", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("=", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.NULL, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		query = "@name !=               NULL";
		parser = new EagleQueryParser(query);
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("NULL", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("!=", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.NULL, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());
	}

	@Test
	public void testIsOrIsNotQuery(){
		String query = "@name is \"james\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("james", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("IS", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.STRING, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		query = "@name is   not \"james\"";
		parser = new EagleQueryParser(query);
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("james", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("IS NOT", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.STRING, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		query = "@name is   1.234";
		parser = new EagleQueryParser(query);
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("1.234", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("IS", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.NUMBER, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		query = "@name is   not 1.234";
		parser = new EagleQueryParser(query);
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("1.234", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("IS NOT", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.NUMBER, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());
	}

	@Test
	public void testINListQuery() {
		String query = "@name in (\"jame\",\"lebron\")";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("(\"jame\",\"lebron\")", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("IN", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.LIST, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		query = "@name NOT IN (1,\"lebron\")";
		parser = new EagleQueryParser(query);
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("(1,\"lebron\")", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("NOT IN", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.LIST, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		query = "@name not                      in (1,\"lebron\")";
		parser = new EagleQueryParser(query);
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("(1,\"lebron\")", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("NOT IN", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.LIST, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());
	}

	@Test
	public void testEmptyString() {
		String query = "@name = \"\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertNotNull(or);
		Assert.assertEquals("@name", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("=", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.STRING, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());
	}

	/**
	 * Will split tokens for escaped string
	 *
	 * "va\"lue" => "va\"lue"
	 * ("va\"lue","va,lue") => ["va\\\"lue","va,lue"]
	 *
	 */
	@Test
	public void testEscapedQuotesString(){
		String query = "@value = \"value\\\"content, and another content\"";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertNotNull(or);
		Assert.assertEquals("@value", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("value\"content, and another content", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("=", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.STRING, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		query = "@value in (\"value\\\"content, and another content\",\"others item\")";
		parser = new EagleQueryParser(query);
		or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertNotNull(or);
		Assert.assertEquals("@value", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("(\"value\\\"content, and another content\",\"others item\")", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("IN", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.LIST, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());

		query = "@value in (\"value\\\"content, and another content\",\"others item\",-1.2345)";
		parser = new EagleQueryParser(query);
		or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertNotNull(or);
		Assert.assertEquals("@value", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("(\"value\\\"content, and another content\",\"others item\",-1.2345)", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("IN", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals(TokenType.LIST, or.getANDExprList().get(0).getAtomicExprList().get(0).getValueType());
	}

	@Test
	public void testCompareAtomicExpression(){
		String query = "EXP{@mapProgress} < EXP{@reduceProgress}";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("mapProgress", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("reduceProgress", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("<", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals("(mapProgress<reduceProgress)", or.toString());
	}

	@Test
	public void testCompareArithmeticExpressionWithNumeric(){
		String query = "EXP{(@mapProgress + @reduceProgress) / (@endTime - @startTime)} < 0.005";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("(mapProgress + reduceProgress) / (endTime - startTime)", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("0.005", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("<", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		LOG.info(or.toString());
		Assert.assertEquals("((mapProgress + reduceProgress) / (endTime - startTime)<0.005)", or.toString());
	}

	@Test
	public void testComplexExpressionWithNestedBrace(){
		String query = "EXP{((@a + @b) / @c) + @d}< 0.005";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		Assert.assertEquals(1, or.getANDExprList().get(0).getAtomicExprList().size());
		Assert.assertEquals("((a + b) / c) + d", or.getANDExprList().get(0).getAtomicExprList().get(0).getKey());
		Assert.assertEquals("0.005", or.getANDExprList().get(0).getAtomicExprList().get(0).getValue());
		Assert.assertEquals("<", or.getANDExprList().get(0).getAtomicExprList().get(0).getOp().toString());
		Assert.assertEquals("(((a + b) / c) + d<0.005)", or.toString());
	}

	@Test
	public void testComplexExpressionWithAndCondition(){
		String query = "(EXP{@a + @b} > 3) AND (@b >10)";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		ANDExpression and = or.getANDExprList().get(0);
		Assert.assertEquals(2, and.getAtomicExprList().size());
		Assert.assertEquals("a + b>3", and.getAtomicExprList().get(0).toString());
		Assert.assertEquals("@b>10", and.getAtomicExprList().get(1).toString());
		
		AtomicExpression leftExpression = and.getAtomicExprList().get(0);
		Assert.assertEquals("a + b", leftExpression.getKey());
		Assert.assertEquals(TokenType.EXP, leftExpression.getKeyType());
		Assert.assertEquals(">", leftExpression.getOp().toString());
		Assert.assertEquals("3", leftExpression.getValue());
		Assert.assertEquals(TokenType.NUMBER, leftExpression.getValueType());
		AtomicExpression rightExpression = and.getAtomicExprList().get(1);
		Assert.assertEquals("@b", rightExpression.getKey());
		Assert.assertEquals(TokenType.ID, rightExpression.getKeyType());
		Assert.assertEquals(">", rightExpression.getOp().toString());
		Assert.assertEquals("10",rightExpression.getValue());
		Assert.assertEquals(TokenType.NUMBER, rightExpression.getValueType());
	}

	@Test
	public void testComplexExpressionWithConditionAndNestedBrace(){
		String query = "(EXP{(@a + @b) / ((@c + @d)*(@e)/(@d))} > EXP{@c + @d}) AND (EXP{@e + @f} > EXP{@h + @i})";
		EagleQueryParser parser = new EagleQueryParser(query);
		ORExpression or = null;
		try{
			or = parser.parse();
		}catch(EagleQueryParseException ex){
			Assert.fail(ex.getMessage());
		}
		Assert.assertTrue(or.getANDExprList().size()==1);
		ANDExpression and = or.getANDExprList().get(0);
		Assert.assertEquals(2, and.getAtomicExprList().size());		
		Assert.assertEquals("(a + b) / ((c + d)*(e)/(d))>c + d", and.getAtomicExprList().get(0).toString());
		Assert.assertEquals("e + f>h + i", and.getAtomicExprList().get(1).toString());
		
		AtomicExpression leftExpression = and.getAtomicExprList().get(0);
		Assert.assertEquals("(a + b) / ((c + d)*(e)/(d))", leftExpression.getKey());
		Assert.assertEquals(">", leftExpression.getOp().toString());
		Assert.assertEquals("c + d", leftExpression.getValue());
				
		AtomicExpression rightExpression = and.getAtomicExprList().get(1);
		Assert.assertEquals("e + f", rightExpression.getKey());
		Assert.assertEquals(">", rightExpression.getOp().toString());
		Assert.assertEquals("h + i",rightExpression.getValue());
	}

	@Test
	public void testNegativeExpressionCase(){
		String query = "(EXP{(@a + @b) / ((@c + @d)*(@e)/(@d))}} > EXP{@c + @d}) AND (EXP{@e + @f} > EXP{@h + @i})";
		EagleQueryParser parser = new EagleQueryParser(query);		
		boolean parseFail = true;
		try{
			parser.parse();
		}catch(EagleQueryParseException ex){
			parseFail = false;			
		}
		Assert.assertFalse(parseFail);
		
		query = "(EXP{{(@a + @b) / ((@c + @d)*(@e)/(@d))}} > EXP{@c + @d}) AND (EXP{@e + @f} > EXP{@h + @i})";
		parser = new EagleQueryParser(query);		
		parseFail = true;
		try{
			parser.parse();
		}catch(EagleQueryParseException ex){
			parseFail = false;			
		}
		Assert.assertFalse(parseFail);

		query = "(EXP{(@a + @b) / ((@c + @d)*(@e)/(@d))} > EXP{@c + @d}) AND EXP{})";
		parser = new EagleQueryParser(query);		
		parseFail = true;
		try{
			parser.parse();
		}catch(EagleQueryParseException ex){
			parseFail = false;			
		}
		Assert.assertFalse(parseFail);

	}

	@Test
	public void testIsExpression(){
		Assert.assertTrue(TokenConstant.isExpression("EXP{ count }"));
		Assert.assertFalse(TokenConstant.isExpression("count"));
	}
}
