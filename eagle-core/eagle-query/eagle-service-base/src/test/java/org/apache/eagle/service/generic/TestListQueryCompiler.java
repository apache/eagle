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
/**
 * 
 */
package org.apache.eagle.service.generic;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestLogAPIEntity;
import org.apache.eagle.query.parser.ORExpression;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.query.ListQueryCompiler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @since Nov 10, 2014
 */
public class TestListQueryCompiler {

	private static final Logger LOG = LoggerFactory.getLogger(TestListQueryCompiler.class);
	
	@Before 
	public void prepare() throws Exception{
		String[] partitions =  new String[2];
		partitions[0] = "cluster";
		partitions[1] = "datacenter";
		EntityDefinitionManager.registerEntity(TestLogAPIEntity.class);
		EntityDefinition entityDef = EntityDefinitionManager.getEntityByServiceName("TestLogAPIEntity");
		entityDef.setPartitions(partitions);
		entityDef.setTimeSeries(true);
	}
	
	 /**************************************************************************************************/
	 /*********************************** Test Expression In List Query*********************************/
	 /**************************************************************************************************/
	
	@Test
	public void testListQueryWithoutExpression() throws Exception{	
		String query = "TestLogAPIEntity[@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND @field5 > 0.05]{@cluster, @field1}";
		ListQueryCompiler compiler = new ListQueryCompiler(query, false);
		ORExpression filter = compiler.getQueryExpression();
		Assert.assertEquals(filter.toString(), "(@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND @field5>0.05)");
		List<String> aggFields = compiler.aggregateFields();
		Assert.assertTrue(aggFields == null);
		List<String> outputFields = compiler.outputFields();
		Assert.assertEquals(outputFields.size(), 2);
		Assert.assertTrue(outputFields.contains("cluster"));
		Assert.assertTrue(outputFields.contains("field1"));
	}
	
	@Test
	public void testListQueryWithExpressionEndWithNumberInFilter() throws Exception{	
		String query = "TestLogAPIEntity[@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND EXP{@field5 + @field6} > 0.05]{@cluster, @field1}";
		ListQueryCompiler compiler = new ListQueryCompiler(query, false);
		ORExpression filter = compiler.getQueryExpression();
		Assert.assertEquals(filter.toString(), "(@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND field5 + field6>0.05)");
		List<String> aggFields = compiler.aggregateFields();
		Assert.assertTrue(aggFields == null);
		List<String> outputFields = compiler.outputFields();
		Assert.assertEquals(outputFields.size(), 2);
		Assert.assertTrue(outputFields.contains("cluster"));
		Assert.assertTrue(outputFields.contains("field1"));
	}
	
	@Test
	public void testListQueryWithExpressionEndWithRPARENInFilter() throws Exception{	
		String query = "TestLogAPIEntity[@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND (EXP{@field5 + @field6} > 0.05)]{@cluster, @field1}";
		ListQueryCompiler compiler = new ListQueryCompiler(query, false);
		ORExpression filter = compiler.getQueryExpression();
		LOG.info(filter.toString());
		Assert.assertEquals(filter.toString(), "(@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND field5 + field6>0.05)");
		List<String> aggFields = compiler.aggregateFields();
		Assert.assertTrue(aggFields == null);
		List<String> outputFields = compiler.outputFields();
		Assert.assertEquals(outputFields.size(), 2);
		Assert.assertTrue(outputFields.contains("cluster"));
		Assert.assertTrue(outputFields.contains("field1"));
		
		query = "TestLogAPIEntity[(@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND (EXP{@field5 + @field6} > 0.05))]{@cluster, @field1}";
		compiler = new ListQueryCompiler(query, false);
		filter = compiler.getQueryExpression();
		Assert.assertEquals(filter.toString(), "(@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND field5 + field6>0.05)");
		aggFields = compiler.aggregateFields();
		Assert.assertTrue(aggFields == null);
		outputFields = compiler.outputFields();
		Assert.assertEquals(outputFields.size(), 2);
		Assert.assertTrue(outputFields.contains("cluster"));
		Assert.assertTrue(outputFields.contains("field1"));
	}
	
	@Test
	public void testListQueryWithExpressionEndWithRBRACEInFilter() throws Exception{			
		String query = "TestLogAPIEntity[@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND EXP{@a + @b} > EXP{0.05 + @c + @d}]{@cluster, EXP{@a + @b}}";
		ListQueryCompiler compiler = new ListQueryCompiler(query, false);
		ORExpression filter = compiler.getQueryExpression();
		Assert.assertEquals(filter.toString(), "(@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND a + b>0.05 + c + d)");
		List<String> aggFields = compiler.aggregateFields();
		Assert.assertTrue(aggFields == null);
		List<String> outputFields = compiler.outputFields();
//		Assert.assertEquals(outputFields.size(), 2);
		Assert.assertTrue(outputFields.contains("cluster"));
		Assert.assertTrue(outputFields.contains("EXP{a + b}"));
	}
	
	/**************************************************************************************************/
	/*********************************** Test Expression In Group By Query*********************************/	
	/**************************************************************************************************/
	
	@Test
	public void testGroupByQueryAggWithoutExpressionInAggFunc() throws Exception{
		String query = "TestLogAPIEntity[@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND EXP{@a + @b} > EXP{@c + @d} AND EXP{@a + @c} < EXP{@b + @d + 0.05}]<@cluster, @datacenter>{sum(@a), avg(@b)}";
		ListQueryCompiler compiler = new ListQueryCompiler(query, false);
		ORExpression filter = compiler.getQueryExpression();
		LOG.info(filter.toString());
		Assert.assertEquals(filter.toString(), "(@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND a + b>c + d AND a + c<b + d + 0.05)");
		
		List<String> groupByFields = compiler.groupbyFields();
		Assert.assertEquals(groupByFields.size(), 2);		
		Assert.assertTrue(groupByFields.contains("cluster"));
		Assert.assertTrue(groupByFields.contains("datacenter"));
		
		List<AggregateFunctionType> functions = compiler.aggregateFunctionTypes();
		Assert.assertEquals(functions.size(), 2);		
		Assert.assertTrue(functions.contains(AggregateFunctionType.sum));
		Assert.assertTrue(functions.contains(AggregateFunctionType.avg));
	
		List<String> aggFields = compiler.aggregateFields();
		Assert.assertEquals(aggFields.size(), 2);
		Assert.assertTrue(aggFields.contains("a"));
		Assert.assertTrue(aggFields.contains("b"));
	}
	
	@Test
	public void testGroupByQueryAggWithExpressionInAggFunc() throws Exception{
		String query = "TestLogAPIEntity[@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND EXP{@a + @b} > EXP{@c + @d} AND EXP{@a + @c} < EXP{@b + @d + 0.07}]<@cluster, @datacenter>{sum(EXP{@a+@b+20.0}), avg(EXP{(@a+@c + 2.5)/@d}), count}";
		ListQueryCompiler compiler = new ListQueryCompiler(query, false);
		ORExpression filter = compiler.getQueryExpression();
		LOG.info(filter.toString());
		Assert.assertEquals(filter.toString(), "(@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND a + b>c + d AND a + c<b + d + 0.07)");
		
		List<String> groupByFields = compiler.groupbyFields();
		Assert.assertEquals(groupByFields.size(), 2);		
		Assert.assertTrue(groupByFields.contains("cluster"));
		Assert.assertTrue(groupByFields.contains("datacenter"));
		
		List<AggregateFunctionType> functions = compiler.aggregateFunctionTypes();
		Assert.assertEquals(functions.size(), 3);		
		Assert.assertTrue(functions.contains(AggregateFunctionType.sum));
		Assert.assertTrue(functions.contains(AggregateFunctionType.avg));
		Assert.assertTrue(functions.contains(AggregateFunctionType.count));
				
		List<String> aggFields = compiler.aggregateFields();
		Assert.assertEquals(aggFields.size(), 3);
		Assert.assertTrue(aggFields.contains("EXP{a+b+20.0}"));
		Assert.assertTrue(aggFields.contains("EXP{(a+c + 2.5)/d}"));
		Assert.assertTrue(aggFields.contains("count"));
	}
	
	/**************************************************************************************************/
	/*********************************** Test Expression In Sort Query*********************************/	
	/**************************************************************************************************/
	
	@Test
	public void testSortQueryWithoutExpressionInSort() throws Exception{
		String query = "TestLogAPIEntity[@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND EXP{@a + @b} > EXP{@c + @d} AND EXP{@a + @c} < EXP{@b + @d}]<@cluster, @datacenter>"
				+ "{sum(@a), count}.{sum(@a) asc}";
		ListQueryCompiler compiler = new ListQueryCompiler(query, false);
		ORExpression filter = compiler.getQueryExpression();
		LOG.info(filter.toString());
		Assert.assertEquals(filter.toString(), "(@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND a + b>c + d AND a + c<b + d)");
		
		List<String> groupByFields = compiler.groupbyFields();
		Assert.assertEquals(groupByFields.size(), 2);		
		Assert.assertTrue(groupByFields.contains("cluster"));
		Assert.assertTrue(groupByFields.contains("datacenter"));
		
		List<AggregateFunctionType> functions = compiler.aggregateFunctionTypes();
		Assert.assertEquals(functions.size(), 2);		
		Assert.assertTrue(functions.contains(AggregateFunctionType.sum));
		Assert.assertTrue(functions.contains(AggregateFunctionType.count));
		
		List<String> aggFields = compiler.aggregateFields();
		Assert.assertEquals(aggFields.size(), 2);
		Assert.assertTrue(aggFields.contains("a"));
		Assert.assertTrue(aggFields.contains("count"));
		
		List<String> sortFields = compiler.sortFields();
		Assert.assertEquals(sortFields.size(), 1);
		Assert.assertTrue(sortFields.contains("a"));
	}
	
	@Test
	public void testSortQuerySortWithExpressionInSort() throws Exception{
		String query = "TestLogAPIEntity[@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND EXP{@a + @b} > EXP{@c + @d} AND EXP{@a + @c} < EXP{@b + @d + 0.05}]<@cluster, @datacenter>"
				+ "{sum(EXP{@a+@b+0.07}), max(EXP{(@a+@c)/@d}), min(EXP{@a+@b})}.{sum(EXP{@a+@b+0.07}) asc}";
		ListQueryCompiler compiler = new ListQueryCompiler(query, false);
		ORExpression filter = compiler.getQueryExpression();
		LOG.info(filter.toString());
		Assert.assertEquals(filter.toString(), "(@cluster=\"cluster\" AND @datacenter=\"datacenter\" AND a + b>c + d AND a + c<b + d + 0.05)");
		
		List<String> groupByFields = compiler.groupbyFields();
		Assert.assertEquals(groupByFields.size(), 2);		
		Assert.assertTrue(groupByFields.contains("cluster"));
		Assert.assertTrue(groupByFields.contains("datacenter"));
		
		List<String> aggFields = compiler.aggregateFields();
		Assert.assertEquals(aggFields.size(), 3);
		Assert.assertTrue(aggFields.contains("EXP{a+b+0.07}"));
		Assert.assertTrue(aggFields.contains("EXP{(a+c)/d}"));
		Assert.assertTrue(aggFields.contains("EXP{a+b}"));
		
		List<AggregateFunctionType> functions = compiler.aggregateFunctionTypes();
		Assert.assertEquals(functions.size(), 3);		
		Assert.assertTrue(functions.contains(AggregateFunctionType.sum));
		Assert.assertTrue(functions.contains(AggregateFunctionType.max));
		Assert.assertTrue(functions.contains(AggregateFunctionType.min));
		
		List<String> sortFields = compiler.sortFields();
		Assert.assertEquals(sortFields.size(), 1);
		Assert.assertTrue(sortFields.contains("EXP{a+b+0.07}"));
	}
}
