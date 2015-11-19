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
package org.apache.eagle.log.entity.filter;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestLogAPIEntity;
import org.apache.eagle.query.parser.EagleQueryParseException;
import org.apache.eagle.query.parser.EagleQueryParser;
import org.apache.eagle.query.parser.ORExpression;
import junit.framework.Assert;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHBaseFilterBuilder {
	private final static Logger LOG = LoggerFactory.getLogger(TestHBaseFilterBuilder.class);
	private EntityDefinition ed;

	private Filter buildFilter(String query) throws EagleQueryParseException {
		ORExpression expression = new EagleQueryParser(query).parse();
		HBaseFilterBuilder builder = new HBaseFilterBuilder(ed,expression);
		Filter filterList =  builder.buildFilters();
		LOG.info("\n" + expression + " \n=> " + filterList);
		return filterList;
	}

	@Before
	public void setUp(){
		try {
			ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
			if(ed == null){
				EntityDefinitionManager.registerEntity(TestLogAPIEntity.class);
				ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestLogAPIEntity.class);
			}
		} catch (InstantiationException e) {
			Assert.fail(e.getMessage());
		} catch (IllegalAccessException e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * Should success without exception
	 */
	@Test
	public void testQueryParseAndBuildFilterSuccess(){
		String[] queries = new String[]{
			"@cluster = \"cluster1\" and @datacenter = \"dc1\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID = \"job_1234\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID = \"PigLatin: \\\"quoted_pig_job_name_value\\\"\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID in (\"job_1234\",\"job_4567\")",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID in (1234,\"job_4567\")",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID in (1234,\"sample job name: \\\"quoted_job_name_value\\\"\")",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID CONTAINS \"job_1234\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID CONTAINS job_1234",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID NOT CONTAINS \"job_456\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID is \"job_789\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID is not \"job_789\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID is null",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID is not null",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID is NULL",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID is not NULL",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID = NULL",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID != null",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID =~ \".*job_1234.*\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID !=~ \".*job_1234.*\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID !=~ \"\\\\|_\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 = 1 ",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 = 1 and @field3 = 100000",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 = 1 and @field5 = 1.56",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 = 1 and @field5 > 1.56",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 = 1 and @field5 >= 1.56",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 = 1 and @field5 < 1.56",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 = 1 and @field5 <= 1.56",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and ( @field3 = 100000 or @field3 < 100000)\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and ( @field3 = 100000 or @field3 in (\"100000\",\"1\"))\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and ( @field3 = 100000 or @field3 in (\"100000\",\"1\"))\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and ( @field3 = 100000 or @field7 in (\"\\\"value1-part1,value1-part2\\\"\",\"value2\"))\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and ( @field3 = 100000 or @field3 not in (\"100000\",\"1\"))\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and ( @field3 = 100000 or @field3 NOT IN (\"100000\",\"1\"))\"",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and ( @field3 = 100000 or @field7 NOT IN (\"\\\"value1-part1,value1-part2\\\"\",\"value2\"))\"",
			// expression filter
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and EXP{field3/field7 - field2} > 12",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field5 > EXP{field3/field7 - field2}",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and EXP{field3/field7 - field2} > EXP{field1 * field2}",
			"@cluster = \"cluster1\" and @datacenter = \"dc1\" and EXP{field3/field7 - field2} > EXP{field1 * field2}",
		};
		for(String query: queries){
			try {
				Filter filter = buildFilter(query);
				Assert.assertNotNull(filter);
			} catch (EagleQueryParseException e) {
				Assert.fail(e.getMessage());
			} catch (Exception ex){
				Assert.fail(ex.getMessage());
			}
		}
	}

	/**
	 * Should throw exception
	 */
	@Test
	public void testNegativeQueryParseSuccessfullyButBuildFilterFailed(){
		String[] queries = new String[]{
				"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @tag < \"job_1234\"",
				"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @tag <= \"job_1234\"",
				"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @tag >= \"job_1234\"",
				"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 < null",
				"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 <= null",
				"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 > NULL",
				"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 >= NULL",
				"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 =~ NULL",
				"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 !=~ NULL",
				"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 contains NULL",
				"@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field1 not contains NULL"
		};
		for(String query: queries){
			try {
				@SuppressWarnings("unused")
				Filter filter = buildFilter(query);
				Assert.fail("Should throw exception: "+query);
			} catch (IllegalArgumentException e) {
				LOG.info("Expect exception: " + e.getMessage());
			} catch (EagleQueryParseException e) {
				Assert.fail("Should parse successfully: "+query);
			}
		}
	}

	@Test
	public void testParsedFilter(){
		String q1 = "@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field3 = 100000";
		try {
			FilterList filterList = (FilterList) buildFilter(q1);
			Assert.assertEquals(FilterList.Operator.MUST_PASS_ONE,filterList.getOperator());
			Assert.assertEquals(1,filterList.getFilters().size());
			Assert.assertEquals(2,((FilterList) filterList.getFilters().get(0)).getFilters().size());
		} catch (EagleQueryParseException e) {
			Assert.fail(e.getMessage());
		}

		String q2 = "@cluster = \"cluster1\" and @datacenter = \"dc1\" and ( @field3 = 100000 or @field3 < 100000)";
		try {
			FilterList filterList = (FilterList) buildFilter(q2);
			Assert.assertEquals(FilterList.Operator.MUST_PASS_ONE,filterList.getOperator());
			Assert.assertEquals(2,filterList.getFilters().size());
			Assert.assertEquals(2,((FilterList) filterList.getFilters().get(0)).getFilters().size());
		} catch (EagleQueryParseException e) {
			Assert.fail(e.getMessage());
		}

		// Test parse success but bad type of value
		String q3 = "@cluster = \"cluster1\" and @datacenter = \"dc1\" and ( @field3 = 100000 or @field3 < \"bad_int_100000\")";
		boolean q3Ex = false;
		try {
			Assert.assertNull(buildFilter(q3));
		} catch (EagleQueryParseException e) {
			Assert.fail(e.getMessage());
		} catch (IllegalArgumentException e){
			LOG.debug("Expect: ", e);
			Assert.assertTrue(e.getCause() instanceof NumberFormatException);
			q3Ex = true;
		}
		Assert.assertTrue(q3Ex);
	}

	@Test
	public void testWithUnescapedString(){
		///////////////////////////////////
		// Tag filter with IN or EQUAL
		// Should use RowKeyFilter only
		///////////////////////////////////
		String query = "@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID = \"job.1234\"";
		try {
			FilterList filter = (FilterList) buildFilter(query);
			Assert.assertEquals(RowFilter.class, ((FilterList) filter.getFilters().get(0)).getFilters().get(0).getClass());
			Assert.assertFalse("Should use rowkey filter only",filter.toString().matches(".*job.1234.*"));
		} catch (EagleQueryParseException e) {
			Assert.fail(e.getMessage());
		} catch (Exception ex){
			Assert.fail(ex.getMessage());
		}

		query = "@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID in (\"job_1234\")";
		try {
			FilterList filter = (FilterList) buildFilter(query);
			Assert.assertEquals(RowFilter.class, ((FilterList) filter.getFilters().get(0)).getFilters().get(0).getClass());
			Assert.assertFalse("Should use rowkey filter only",filter.toString().matches(".*job_1234.*"));
		} catch (EagleQueryParseException e) {
			Assert.fail(e.getMessage());
		} catch (Exception ex){
			Assert.fail(ex.getMessage());
		}

		query = "@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID in (\"job.1234\")";
		try {
			FilterList filter = (FilterList) buildFilter(query);
			Assert.assertEquals(RowFilter.class, ((FilterList) filter.getFilters().get(0)).getFilters().get(0).getClass());
			Assert.assertFalse("Should use rowkey filter only",filter.toString().matches(".*job.*1234.*"));
		} catch (EagleQueryParseException e) {
			Assert.fail(e.getMessage());
		} catch (Exception ex){
			Assert.fail(ex.getMessage());
		}

		///////////////////////////////
		// Tag with other operators
		///////////////////////////////
		query = "@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID =~ \"job_1234\"";

		try {
			FilterList filter = (FilterList) buildFilter(query);
			Assert.assertEquals(RowFilter.class, ((FilterList) filter.getFilters().get(0)).getFilters().get(0).getClass());
			Assert.assertTrue(filter.toString().matches(".*job_1234.*"));
		} catch (EagleQueryParseException e) {
			Assert.fail(e.getMessage());
		} catch (Exception ex){
			Assert.fail(ex.getMessage());
		}

		query = "@cluster = \"cluster1\" and @datacenter = \"dc1\" and @jobID =~ \"job.1234\"";

		try {
			FilterList filter = (FilterList) buildFilter(query);
			Assert.assertEquals(RowFilter.class, ((FilterList) filter.getFilters().get(0)).getFilters().get(0).getClass());
			Assert.assertTrue(filter.toString().matches(".*job.1234.*"));
		} catch (EagleQueryParseException e) {
			Assert.fail(e.getMessage());
		} catch (Exception ex){
			Assert.fail(ex.getMessage());
		}

		///////////////////////////////
		// Tag with IN
		// Should escape regexp chars
		///////////////////////////////
		query = "@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field7 = \"job_1234\"";

		try {
			FilterList filter = (FilterList) buildFilter(query);
			Assert.assertEquals(RowFilter.class, ((FilterList) filter.getFilters().get(0)).getFilters().get(0).getClass());
			Assert.assertTrue(filter.toString().matches(".*job_1234.*"));
		} catch (EagleQueryParseException e) {
			Assert.fail(e.getMessage());
		} catch (Exception ex){
			ex.printStackTrace();
			Assert.fail(ex.getMessage());
		}

		query = "@cluster = \"cluster1\" and @datacenter = \"dc1\" and @field7 in (\"job.1234\",\"others\")";

		try {
			FilterList filter = (FilterList) buildFilter(query);
			Assert.assertEquals(RowFilter.class, ((FilterList) filter.getFilters().get(0)).getFilters().get(0).getClass());
			Assert.assertTrue(filter.toString().matches(".*job\\.1234.*"));
		} catch (EagleQueryParseException e) {
			Assert.fail(e.getMessage());
		} catch (Exception ex){
			Assert.fail(ex.getMessage());
		}
	}
}
