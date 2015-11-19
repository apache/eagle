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
package org.apache.eagle.security.hive.ql;

import static org.junit.Assert.*;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestParser {
  //private static final Logger LOG = LoggerFactory.getLogger(TestParser.class);
  Parser parser;

  @Before
  public void setUp() {
    parser = new Parser();
  }
  private void _testParsingQuery(String query,
                                 String expectedOperation,
                                 String expectedInsertTable,
                                 Map<String, Set<String>> expectedTableColumn)
                                     throws Exception {
    HiveQLParserContent content = new HiveQLParserContent();
    content = parser.run(query);

    Assert.assertTrue("Query operations are not matched.",
        content.getOperation().equals(expectedOperation));
    Assert.assertTrue("Insert tables are not matched.",
        content.getInsertTable().equals(expectedInsertTable));
    //Assert.assertTrue("Table and column mapping is incorrect.",
    //    content.getTableColumnMap().equals(expectedTableColumn));
  }

  private void printTree(ASTNode root, int indent) {
    if ( root != null ) {
      StringBuffer sb = new StringBuffer(indent);
      for ( int i = 0; i < indent; i++ )
        sb = sb.append("   ");
      for ( int i = 0; i < root.getChildCount(); i++ ) {
        System.out.println(sb.toString() + root.getChild(i).getText());
        printTree((ASTNode)root.getChild(i), indent + 1);
      }
    }
  }

  public void printQueryAST(String query) throws ParseException {
    ASTNode root = parser.generateAST(query);
    printTree(root, 0);
  }

  @Test
  public void testSelectStatment() throws Exception {
    String query = "select * from cts_common_prod_sd_2015060600_ed_2015071300 "
        + "where partner=965704 and brand_id=14623 "
        + "and date_key>=2015071400 and date_key<=2015071300";
    String expectedOperation = "SELECT";
    String expectedInsertTable = "TOK_TMP_FILE";
    Map<String, Set<String>> expectedTableColumn = new HashMap<String, Set<String>>();
    Set<String> set = new HashSet<String>();
    set.add("*");
    expectedTableColumn.put("cts_common_prod_sd_2015060600_ed_2015071300", set);

    _testParsingQuery(query, expectedOperation, expectedInsertTable, expectedTableColumn);
  }
  
  //@Test
  public void testSelectDistinctStatement() throws Exception {
    String query = "select distinct action_timestamp,exchange_id "
        + "from chango_ica_new "
        + "where action='IM' and dt='20150615'";
    String expectedOperation = "SELECT DISTINCT";
    String expectedInsertTable = "TOK_TMP_FILE";
    Map<String, Set<String>> expectedTableColumn = new HashMap<String, Set<String>>();
    Set<String> set = new HashSet<String>();
    set.add("action_timestamp");
    set.add("exchange_id");
    expectedTableColumn.put("chango_ica_new", set);

    _testParsingQuery(query, expectedOperation, expectedInsertTable, expectedTableColumn);
  }

  @Test
  public void testInsertStatement() throws Exception {
    String query = "INSERT OVERWRITE TABLE myflightinfo "
        + "PARTITION (Month=1) "
        + "SELECT Year, Month, DayofMonth "
        + "FROM FlightInfo2008 WHERE Month=1";
    String expectedOperation = "SELECT";
    String expectedInsertTable = "myflightinfo";
    Map<String, Set<String>> expectedTableColumn = new HashMap<String, Set<String>>();
    Set<String> set = new HashSet<String>();
    set.add("Year");
    set.add("Month");
    set.add("DayofMonth");
    expectedTableColumn.put("FlightInfo2008", set);

    _testParsingQuery(query, expectedOperation, expectedInsertTable, expectedTableColumn);
  }

  //@Test
  public void testSelectExprStatement() throws Exception {
    String query = "INSERT OVERWRITE TABLE top_level_viewer_dpms select scandate , pathtype , pathname , pathlevel , spacesize * 3 , diskspacequota , 0 pathsize_increase , namespacequota , filecount , dircount , username , groupname, 'XYZ' system from hdfsdu where asofdate = '20150908' and pathlevel <= 3";
    String expectedOperation = "INSERT";
    String expectedInsertTable = "top_level_viewer_dpms";
    Map<String, Set<String>> expectedTableColumn = new HashMap<String, Set<String>>();
    _testParsingQuery(query, expectedOperation, expectedInsertTable, expectedTableColumn);
  }

  @Test
  public void testAliaTableStatement() throws Exception {
    String query = "select a.phone_number  from customer_details a, call_detail_records b where a.phone_number=b.phone_number";
    String expectedOperation = "SELECT";
    String expectedInsertTable = "TOK_TMP_FILE";
    Map<String, Set<String>> expectedTableColumn = new HashMap<String, Set<String>>();
    _testParsingQuery(query, expectedOperation, expectedInsertTable, expectedTableColumn);
  }

  @Test
  public void testAliaColumnStatement() throws Exception {
    String query = "SELECT upper(name), salary, deductions[\"Federal Taxes\"] as fed_taxes, round(salary * (1 - deductions[\"Federal Taxes\"])) as salary_minus_fed_taxes FROM employees LIMIT 2";
    String expectedOperation = "SELECT";
    String expectedInsertTable = "TOK_TMP_FILE";
    Map<String, Set<String>> expectedTableColumn = new HashMap<String, Set<String>>();
    _testParsingQuery(query, expectedOperation, expectedInsertTable, expectedTableColumn);
  }

}
