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

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class Parser {
  private static final Logger LOG = LoggerFactory.getLogger(Parser.class);

  private Set<String> tableSet;
  private Set<String> columnSet;
  private Map<String, String> columnAliasMap;
  private Map<String, String> tableAliasMap;
  private HiveQLParserContent parserContent;

  public Parser() {
    tableSet = new HashSet<String>();
    columnSet = new HashSet<String>();
    tableAliasMap = new HashMap<String, String>();
    columnAliasMap = new HashMap<String, String>();
    parserContent = new HiveQLParserContent();
  }

  /**
   * 
   * @param query
   * @return
   * @throws Exception
   */
  public HiveQLParserContent run(String query) throws Exception {
    ASTNode tree = generateAST(query);
    parseQL((ASTNode)tree.getChild(0));

    LOG.info("HiveQL parse completed.");

    return parserContent;
  }

  /** 
   * Parse an Hive QL into an Abstract Syntax Tree(AST).
   * @param query
   * @return
   * @throws ParseException
   */
  public ASTNode generateAST(String query) throws ParseException {
    ParseDriver pd = new ParseDriver();
    return pd.parse(query);
  }

  private void parseQL(ASTNode ast) {
    switch (ast.getType()) {
    case HiveParser.TOK_QUERY:
      visitSubtree(ast);
      break;

    case HiveParser.TOK_UPDATE_TABLE:
      setOperation("UPDATE");
      visitSubtree(ast);
      break;

    case HiveParser.TOK_DELETE_FROM:
      setOperation("DELETE FROM");
      visitSubtree(ast);
      break;

    default:
      LOG.error("Unsupporting query operation " + ast.getType());
      throw new IllegalStateException("Query operation is not supported "
          + ast.getType());
    }
  }

  private void visitSubtree(ASTNode ast) {
    int len = ast.getChildCount();
    if (len > 0) {
      for (Node n : ast.getChildren()) {
        ASTNode asn = (ASTNode)n;
        switch (asn.getToken().getType()) {
        case HiveParser.TOK_TABNAME:
          tableSet.add(ast.getChild(0).getChild(0).getText());
          break;
        case HiveParser.TOK_SET_COLUMNS_CLAUSE:
          for (int i = 0; i < asn.getChildCount(); i++) {
            addToColumnSet((ASTNode)asn.getChild(i).getChild(0));
          }
        case HiveParser.TOK_FROM:
          parseFromClause((ASTNode)asn.getChild(0));
          break;
        case HiveParser.TOK_INSERT:
          for (int i = 0; i < asn.getChildCount(); i++) {
            parseInsertClause((ASTNode)asn.getChild(i));                           
          }
          break;
        case HiveParser.TOK_UNIONTYPE: 
          int childcount = asn.getChildCount();
          for (int i = 0; i < childcount; i++) {    
            parseQL((ASTNode)asn.getChild(i));
          }
          break;
        }
      }

      // Add tableSet and columnSet to tableColumnMap
      addTablesColumnsToMap(tableSet, columnSet);
    }
  }
  
  private void parseSubQuery(ASTNode subQuery) {

    int cc = 0;
    int cp = 0;

    switch (subQuery.getToken().getType()) {
    case HiveParser.TOK_QUERY:
      visitSubtree(subQuery);
      break;
    case HiveParser.TOK_UNIONTYPE: 
      cc = subQuery.getChildCount();

      for ( cp = 0; cp < cc; ++cp) {    
        parseSubQuery((ASTNode)subQuery.getChild(cp));
      }
      break;
    }
  }

  private void parseInsertClause(ASTNode ast) {
    switch(ast.getToken().getType()) {
    case HiveParser.TOK_DESTINATION:
    case HiveParser.TOK_INSERT_INTO:
      if (ast.getChild(0).getType() == HiveParser.TOK_DIR) {
        String table = ast.getChild(0).getChild(0).getText();
        setInsertTable(table);
      } else {
        String table = ast.getChild(0).getChild(0).getChild(0).getText();
        setInsertTable(table);
      }
      break;

    case HiveParser.TOK_SELECT:
    case HiveParser.TOK_SELECTDI:
      setOperation("SELECT");
      parseSelectClause(ast);
      break;

    }
  }

  private void parseFromClause(ASTNode qf) {

    int cc = 0;
    int cp = 0;

    switch (qf.getToken().getType()) {    
    case HiveParser.TOK_TABREF:
      ASTNode atmp = (ASTNode)qf.getChild(0);
      String tb = atmp.getChildCount()==1 ? atmp.getChild(0).toString() 
          : atmp.getChild(0).toString()  + "." + atmp.getChild(1).toString() ;
      tableSet.add(tb);
      if (qf.getChildCount() > 1) {
        String child_0 = tb;
        String child_1 = qf.getChild(1).toString();
        tableAliasMap.put(child_1, child_0);
      }

      break;

    case HiveParser.TOK_LEFTOUTERJOIN:
      cc = qf.getChildCount();

      for ( cp = 0; cp < cc; cp++) {    
        ASTNode atm = (ASTNode)qf.getChild(cp);
        parseFromClause(atm);
      }
      break;

    case HiveParser.TOK_JOIN:
      cc = qf.getChildCount();

      for ( cp = 0; cp < cc; cp++) {    
        ASTNode atm = (ASTNode)qf.getChild(cp);
        parseFromClause(atm);
      }
      break;  
    case HiveParser.TOK_SUBQUERY:

      parseSubQuery((ASTNode)qf.getChild(0));
      break;   
    case HiveParser.TOK_LATERAL_VIEW:
      cc = qf.getChildCount();
      for ( cp = 0; cp < cc; ++cp) {    
        parseFromClause((ASTNode)qf.getChild(cp));
      }
      break;                                                          
    }
  }
  
  private void parseSelectClause(ASTNode ast) {
    for (int i = 0; i < ast.getChildCount(); i++) {
      ASTNode astmp = (ASTNode)ast.getChild(i);
      switch (astmp.getChild(0).getType()) {
      case HiveParser.TOK_TABLE_OR_COL:
        addToColumnSet(astmp);
        break;

      case HiveParser.TOK_ALLCOLREF:
        String tableName;
        ASTNode node = (ASTNode)astmp.getChild(0).getChild(0);
        if (node != null && node.getType() == HiveParser.TOK_TABNAME) {
          String strTemp = node.getChild(0).getText();
          tableName = convAliasToReal(tableAliasMap, strTemp);
          Set<String> cols = new HashSet<String>();
          cols.add("*");
          parserContent.getTableColumnMap().put(tableName, cols);
        } else {
          columnSet = new HashSet<String>();
          columnSet.add("*");
          for (String tb : tableSet) {
            parserContent.getTableColumnMap().put(tb, columnSet);
          }
        }
        break;

      case HiveParser.TOK_FUNCTION:
        // Traverse children to get TOK_TABLE_OR_COL
        Set<String> tempSet = new HashSet<String>();
        parseTokFunction(ast, tempSet);
        columnSet.addAll(tempSet);
        break;

      case HiveParser.TOK_FUNCTIONSTAR:
        break;

      default:
        if(astmp.getChild(0).getText().equals(".")) {
          String tbAlias = astmp.getChild(0).getChild(0).getChild(0).getText();
          tableName = convAliasToReal(tableAliasMap, tbAlias);
          String strTemp = astmp.getChild(0).getChild(1).getText();
          String col = convAliasToReal(columnAliasMap, strTemp);
          addTableColumnToMap(tableName, col);
        }
        else {
          tempSet = new HashSet<String>();
          parseTokFunction(astmp, tempSet);
          columnSet.addAll(tempSet);
        }
      }
    }
  }

  private void parseTokFunction(ASTNode ast, Set<String> set) {
    if (ast.getType() == HiveParser.TOK_TABLE_OR_COL) {
      String colRealName = convAliasToReal(columnAliasMap, ast.getChild(0).getText());
      set.add(colRealName);
    }
    for (int i = 0; i < ast.getChildCount(); i++) {
      ASTNode n = (ASTNode)ast.getChild(i);
      if (n != null) {
          parseTokFunction(n, set);
      }
    }
  }

  private void addToColumnSet(ASTNode node) {
    ASTNode child_0 = (ASTNode)node.getChild(0).getChild(0);
    ASTNode child_1 = (ASTNode)node.getChild(1);
    if (child_1 != null) {
      columnAliasMap.put(child_1.getText(), child_0.getText());
    }
    String col = convAliasToReal(columnAliasMap, child_0.getText());
    columnSet.add(col);
  }

  private void addTableColumnToMap(String tb, String col) {
    Set<String> columnList;
    Set<String> temp = parserContent.getTableColumnMap().get(tb);
    if (temp != null) {
      columnList = new HashSet <String>(temp);
    } else {
      columnList = new HashSet<String>();
    }
    columnList.add(col);
    parserContent.getTableColumnMap().put(tb, columnList);
  }

  @SuppressWarnings("null")
  private void addTablesColumnsToMap(Set<String> tbs, Set<String> cols) {
    Map<String, Set<String>> map = parserContent.getTableColumnMap();
    for (String tb : tbs) {
      if (map != null) {
        Set<String> temp = map.get(tb);
        if (temp != null) {
          Set<String> value = new HashSet<String>(temp);
          value.addAll(cols);
          map.put(tb, value);
        } else {
          map.put(tb, cols);
        }
      } else {
        map.put(tb, cols);
      }
    }
  }

  private String convAliasToReal(Map<String, String> map, String str) {
    String name = map.get(str);
    if (name != null) {
      return name;
    }
    return str;
  }

  private void setOperation(String opt) {
    parserContent.setOperation(opt);
  }

  private void setInsertTable(String table) {
    parserContent.setInsertTable(table);
  }
}
