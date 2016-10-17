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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

    Assert.assertEquals("Query operations are not matched.",
        content.getOperation(), expectedOperation);
    if(content.getInsertTable() != null && expectedInsertTable != null) {
        Assert.assertEquals("Insert tables are not matched.",
                content.getInsertTable(), expectedInsertTable);
    }
    if(content.getTableColumnMap() != null && expectedTableColumn != null) {
        Assert.assertEquals("Table and column mapping is incorrect.",
                content.getTableColumnMap(), expectedTableColumn);
    }
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

  public void printQueryAST(String query) throws RecognitionException {
    ASTNode root = parser.generateAST(query);
    printTree(root, 0);
  }

  @Test
  public void testSelectStatment() throws Exception {
    String query = "select * from t1 "
        + "where partner=965704 and brand_id=14623 "
        + "and date_key>=2015071400 and date_key<=2015071300";
    String expectedOperation = "SELECT";
    String expectedInsertTable = "TOK_TMP_FILE";
    Map<String, Set<String>> expectedTableColumn = new HashMap<String, Set<String>>();;
    Set<String> set = new HashSet<String>();
    set.add("*");
    expectedTableColumn.put("t1", set);

    _testParsingQuery(query, expectedOperation, expectedInsertTable, expectedTableColumn);
  }
  
  //@Test
  public void testSelectDistinctStatement() throws Exception {
    String query = "select distinct action_timestamp,exchange_id "
        + "from t1 "
        + "where action='IM' and dt='20150615'";
    String expectedOperation = "SELECT DISTINCT";
    String expectedInsertTable = "t1";
    Map<String, Set<String>> expectedTableColumn = null;
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
    Map<String, Set<String>> expectedTableColumn = new HashMap<String, Set<String>>();;
    Set<String> set = new HashSet<String>();
    set.add("Year");
    set.add("Month");
    set.add("DayofMonth");
    expectedTableColumn.put("FlightInfo2008", set);

    _testParsingQuery(query, expectedOperation, expectedInsertTable, expectedTableColumn);
  }

  @Test
  public void testSelectExprStatement() throws Exception {
    String query = "INSERT OVERWRITE TABLE t1 select scandate , pathtype , pathname , pathlevel , spacesize * 3 , diskspacequota , pathsize_increase , namespacequota , filecount , dircount , username , groupname, 'XYZ' system from hdfsdu where asofdate = '20150908' and pathlevel <= 3";
    String expectedOperation = "SELECT";
    String expectedInsertTable = "t1";
    Map<String, Set<String>> expectedTableColumn = null;
    _testParsingQuery(query, expectedOperation, expectedInsertTable, expectedTableColumn);
  }

  @Test
  public void testAliaTableStatement() throws Exception {
    String query = "select a.phone_number from t1 a, t2 b where a.phone_number=b.phone_number";
    String expectedOperation = "SELECT";
    String expectedInsertTable = "TOK_TMP_FILE";
    Map<String, Set<String>> expectedTableColumn = null;
    _testParsingQuery(query, expectedOperation, expectedInsertTable, expectedTableColumn);
  }

  @Test
  public void testAliaColumnStatement() throws Exception {
    String query = "SELECT upper(name), salary, deductions[\"Federal Taxes\"] as fed_taxes, round(salary * (1 - deductions[\"Federal Taxes\"])) as salary_minus_fed_taxes FROM employees LIMIT 2";
    String expectedOperation = "SELECT";
    String expectedInsertTable = "TOK_TMP_FILE";
    Map<String, Set<String>> expectedTableColumn = null;
    _testParsingQuery(query, expectedOperation, expectedInsertTable, expectedTableColumn);
  }

    @Test
    public void testFromStatement1() throws Exception {
        String query = "INSERT OVERWRITE TABLE t1.tt1 PARTITION ( dt='20151121')\n" +
                "select distinct user_id, concat(categ_id,get_json_object(dep3, '$.categ_id'), level_id, get_json_object(dep3, '$.level_id'), site_id, get_json_object(dep3, '$.site_id')) from (\n" +
                "select user_id, if(instr(dep2, \"name\")>0, get_json_object(dep2, '$.vq_rank'), dep2) dep3 from (\n" +
                "select user_id, if(instr(dep1, \"name\")>0, concat(dep1, \"}\"), dep1) dep2\n" +
                "from (\n" +
                "select user_id , split(regexp_replace(regexp_replace(department, \"\\\\[|\\\\]\", \"\"), \"},\" , \"}|\"), \"\\\\|\") as dep\n" +
                "from\n" +
                "(select user_id, BEAvroParser(record_value)['department'] as department\n" +
                "from p13n_user_hour_queue\n" +
                "where dt between '20151121' and '20151122'\n" +
                "and schema_name = 'cust_dna_vq_cat_feed') a ) b\n" +
                "lateral view outer explode(dep) c as dep1) d ) e";
        String expectedOperation = "SELECT";
        String expectedInsertTable = "t1";
        Map<String, Set<String>> expectedTableColumn = null;
        _testParsingQuery(query, expectedOperation, expectedInsertTable, expectedTableColumn);
    }

    @Test
    public void testFromStatement2() throws Exception {
        String query = "insert overwrite table t1\n" +
                "SELECT dt,cobrand,device_type,geo_ind,byr_region,slr_id,slr_region,item_format,price_tranche,vertical,sap_category_id,site_id,user_seg,sort,page_id,page_number,item_rank,relist_flag,app_name,SUM(impr_cnt) impr_cnt\n" +
                "FROM ( SELECT\n" +
                "dt,\n" +
                "cobrand,\n" +
                "device_type,\n" +
                "geo_ind,\n" +
                "byr_region,\n" +
                "slr_id,\n" +
                "slr_region,\n" +
                "item_format,\n" +
                "price_tranche,\n" +
                "vertical,\n" +
                "sap_category_id,\n" +
                "site_id,\n" +
                "user_seg,\n" +
                "sort,\n" +
                "page_id,\n" +
                "page_number,\n" +
                "item_rank,\n" +
                "relist_flag,\n" +
                "item_rank,\n" +
                "relist_flag,\n" +
                "app_name,\n" +
                "impr_cnt\n" +
                "FROM  (SELECT * FROM t2 WHERE user_id > 0) a\n" +
                "LEFT JOIN t3 b\n" +
                "ON a.user_id=b.user_id\n" +
                "UNION ALL\n" +
                "SELECT dt,cobrand,device_type,geo_ind,byr_region,slr_id,slr_region,item_format,price_tranche,vertical,sap_category_id,site_id,'NA' AS user_seg,sort,page_id,page_number,item_rank,relist_flag,app_name,impr_cnt\n" +
                "FROM t3 WHERE user_id < 0\n" +
                ")a";
        String expectedOperation = "SELECT";
        String expectedInsertTable = "t1";
        Map<String, Set<String>> expectedTableColumn = null;
        _testParsingQuery(query, expectedOperation, expectedInsertTable, expectedTableColumn);
    }

    @Test
    public void testCreateTable() throws Exception {
        String query = "CREATE TABLE page_view(viewTime INT, userid BIGINT,\n" +
                "                page_url STRING, referrer_url STRING,\n" +
                "                ip STRING COMMENT 'IP Address of the User')\n" +
                "COMMENT 'This is the page view table'\n" +
                "PARTITIONED BY(dt STRING, country STRING)\n" +
                "STORED AS SEQUENCEFILE";
        String expectedOperation = "CREATE";
        _testParsingQuery(query, expectedOperation, null, null);
    }

    @Test
    public void testCreateTable2() throws Exception {
        String query = "CREATE TABLE t2 as\n" +
                "        SELECT\n" +
                "                user_id,\n" +
                "                max(ts) as max_ts\n" +
                "        FROM\n" +
                "                t1\n" +
                "        GROUP BY user_id";
        String expectedOperation = "SELECT";
        _testParsingQuery(query, expectedOperation, null, null);

    }

    @Test
    public void testAlertTable() throws Exception {
        String query = "ALTER TABLE t1 DROP PARTITION (ds='2008-08-08')";
        String expectedOperation = "ALTER";
        _testParsingQuery(query, expectedOperation, null, null);
    }

    @Test
    public void testDropTable() throws Exception {
        String query = "DROP TABLE t1";
        String expectedOperation = "DROP";
        _testParsingQuery(query, expectedOperation, null, null);
    }

    @Test
    public void testUnionAll() throws Exception {
        String query = "INSERT OVERWRITE TABLE t1 PARTITION ( dt='20151125', hour='06') select a.uid ,a.site_id ,a.page_id ,a.curprice ,a.itm ,a.itmcond ,a.itmtitle ,a.l1 ,a.l2 ,a.leaf ,a.meta ,a.st ,a.dc ,a.tr ,a.eventtimestamp ,a.cln ,a.siid ,a.ciid ,a.sellerid ,a.pri from (select a1.* from (select * from soj_view_event where dt='20151125' and hour='06') a1 inner join (select uid from t2 where dt='20151125' and hour='06') b1 on a1.uid = b1.uid) a left outer join (select c.* from (select * from t3 where dt='20151125' and hour='06') a2 lateral view json_tuple(a2.values2, 'u', 'site_id', 'p', 'current_price', 'item_id', 'item_condition', 'item_title', 'l1_cat_id', 'l2_cat_id', 'leaf_cat_id', 'meta_cat_id','sale_type_enum', 'shipping_country_id', 'time_remain_secs', 'timestamp', 'collection_id', 'source_impression_id', 'current_impression_id', 'sellerid' ) c as uid, site_id, page_id, curprice, itm, itmcond, itmtitle, l1, l2, leaf, meta, st, dc, tr, eventtimestamp, cln, siid, ciid, sellerid ) b on a.uid = b.uid and a.site_id = b.site_id and a.page_id = b.page_id and coalesce(a.curprice, 'null') = coalesce(b.curprice, 'null') and coalesce(a.itm, 'null') = coalesce(b.itm, 'null') and coalesce(a.itmcond, 'null') = coalesce(b.itmcond, 'null') and coalesce(trim(reflect(\"java.net.URLDecoder\", \"decode\",regexp_replace(a.itmtitle,\"\\\\+\",\" \"),\"utf-8\")),'null') = coalesce(trim(b.itmtitle),'null') and coalesce(a.l1, 'null') = coalesce(b.l1, 'null') and coalesce(a.l2,'null') = coalesce(b.l2,'null') and coalesce(a.leaf,'null') = coalesce(b.leaf,'null') and coalesce(a.meta,'null') = coalesce(b.meta,'null') and coalesce(a.st,'null') = coalesce(b.st,'null') and coalesce(a.dc,'null') = coalesce(b.dc,'null') and coalesce(reflect(\"java.net.URLDecoder\", \"decode\",a.tr,\"utf-8\"),'null') = coalesce(b.tr,'null') and a.eventtimestamp = b.eventtimestamp and coalesce(a.cln,'null') = coalesce(b.cln,'null') and coalesce(a.siid,'null') = coalesce(b.siid,'null') and coalesce(a.ciid,'null') = coalesce(b.ciid,'null') and coalesce(a.sellerid, 'null') = coalesce(b.sellerid, 'null') where b.uid is null distribute by a.uid sort by a.uid , a.eventtimestamp";
        
        String expectedOperation = "SELECT";
        String expectedInsertTable = "t1";
        _testParsingQuery(query, expectedOperation, expectedInsertTable, null);
    }

    @Test
    public void testLateralView() throws Exception {
        String query = "select game_id, user_id from t1 lateral view explode(split(userl_ids,'\\\\[\\\\[\\\\[')) snTable as user_id";
        String expectedOperation = "SELECT";
        String expectedInsertTable = "TOK_TMP_FILE";
        _testParsingQuery(query, expectedOperation, expectedInsertTable, null);
    }

}
