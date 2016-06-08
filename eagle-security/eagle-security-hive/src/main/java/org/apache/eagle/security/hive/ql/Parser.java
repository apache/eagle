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

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTErrorNode;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class Parser {
  private static final Logger LOG = LoggerFactory.getLogger(Parser.class);

  static final TreeAdaptor adaptor = new CommonTreeAdaptor() {
    @Override
    public Object create(Token payload) {
      return new ASTNode(payload);
    }
    @Override
    public Object dupNode(Object t) {
      return create(((CommonTree)t).token);
    }
    @Override
    public Object errorNode(TokenStream input, Token start, Token stop, RecognitionException e) {
      return new ASTErrorNode(input, start, stop, e);
    }
  };

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
   * @throws RecognitionException
   */
  public ASTNode generateAST(String query) throws RecognitionException {
    // https://issues.apache.org/jira/browse/HIVE-10731
    // https://issues.apache.org/jira/browse/HIVE-6617
    HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_SQL11_RESERVED_KEYWORDS, false);

    ParseDriver pd = new ParseDriver();
    ParseDriver.ANTLRNoCaseStringStream antlrStream = pd.new ANTLRNoCaseStringStream(query);
    ParseDriver.HiveLexerX lexer = pd.new HiveLexerX(antlrStream);
    lexer.setHiveConf(hiveConf);
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);

    HiveParser parser = new HiveParser(tokens);
    parser.setHiveConf(hiveConf);
    parser.setTreeAdaptor(adaptor);
    HiveParser.statement_return r = parser.statement();

    return (ASTNode)r.getTree();
  }

  private void parseQL(ASTNode ast) {
    switch (ast.getType()) {
    case HiveParser.TOK_QUERY:
      parseQueryClause(ast);
      addTablesColumnsToMap(tableSet, columnSet);
      break;

    case HiveParser.TOK_UPDATE_TABLE:
      setOperation("UPDATE");
      visitSubtree(ast);
      break;

    case HiveParser.TOK_DELETE_FROM:
      setOperation("DELETE");
      visitSubtree(ast);
      break;

    case HiveParser.TOK_CREATETABLE:
      setOperation("CREATE");
      visitSubtree(ast);
      break;

    case HiveParser.TOK_DROPTABLE:
      setOperation("DROP");
      visitSubtree(ast);
      break;

    case HiveParser.TOK_ALTERTABLE:
      setOperation("ALTER");
      visitSubtree(ast);
      break;

    default:
      LOG.error("Unsupported query operation " + ast.getText());
      throw new IllegalStateException("Query operation is not supported "
          + ast.getText());
    }
  }

  private void visitSubtree(ASTNode ast) {
    int len = ast.getChildCount();
    if (len > 0) {
      for (Node n : ast.getChildren()) {
        ASTNode asn = (ASTNode)n;
        switch (asn.getToken().getType()) {
          case HiveParser.TOK_TABNAME:
            //tableSet.add(ast.getChild(0).getChild(0).getText());
            parserContent.getTableColumnMap().put(ast.getChild(0).getChild(0).getText(), new HashSet<>(columnSet));
            break;
          case HiveParser.TOK_SET_COLUMNS_CLAUSE:
            for (int i = 0; i < asn.getChildCount(); i++) {
              addToColumnSet((ASTNode) asn.getChild(i).getChild(0));
            }
            break;
          case HiveParser.TOK_QUERY:
            parseQueryClause(asn);
            break;
          case HiveParser.TOK_UNIONTYPE:
          case HiveParser.TOK_UNIONALL:
          case HiveParser.TOK_UNIONDISTINCT:
            visitSubtree(asn);
            break;
        }
      }
      // Add tableSet and columnSet to tableColumnMap
      addTablesColumnsToMap(tableSet, columnSet);
    }
  }

  private void parseQueryClause(ASTNode ast) {
    int len = ast.getChildCount();
    if (len > 0) {
      for (Node n : ast.getChildren()) {
        ASTNode asn = (ASTNode) n;
        switch (asn.getToken().getType()) {
          case HiveParser.TOK_FROM:
            parseFromClause((ASTNode) asn.getChild(0));
            break;
          case HiveParser.TOK_INSERT:
            for (int i = 0; i < asn.getChildCount(); i++) {
              parseInsertClause((ASTNode) asn.getChild(i));
            }
            break;
        }
      }
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
      visitSubtree(qf);
      break;

    case HiveParser.TOK_LATERAL_VIEW:
    case HiveParser.TOK_LATERAL_VIEW_OUTER:
      cc = qf.getChildCount();
      for ( cp = 0; cp < cc; ++cp) {    
        parseFromClause((ASTNode)qf.getChild(cp));
      }
      break;
    }
  }
  
  private void parseSelectClause(ASTNode ast) {
    for (int i = 0; i < ast.getChildCount(); i++) {
      ASTNode selectXpr = (ASTNode)ast.getChild(i);
      for(int j = 0; j < selectXpr.getChildCount(); j++) {
        parseSelectExpr((ASTNode)selectXpr.getChild(j));
      }
    }
  }

  private void parseSelectExpr(ASTNode asn) {
    switch (asn.getType()) {
      case HiveParser.TOK_TABLE_OR_COL:
        addToColumnSet((ASTNode)asn.getParent());
        break;

      case HiveParser.TOK_ALLCOLREF:
        String tableName;
        ASTNode node = (ASTNode)asn.getChild(0);
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

      case HiveParser.TOK_FUNCTION: case HiveParser.TOK_FUNCTIONDI:
        // Traverse children to get TOK_TABLE_OR_COL
        Set<String> tempSet = new HashSet<>();
        parseTokFunction(asn, tempSet);
        columnSet.addAll(tempSet);
        break;

      case HiveParser.TOK_FUNCTIONSTAR:
        break;

      case HiveParser.DOT:
        String tbAlias = asn.getChild(0).getChild(0).getText();
        tableName = convAliasToReal(tableAliasMap, tbAlias);
        String strTemp = asn.getChild(1).getText();
        String col = convAliasToReal(columnAliasMap, strTemp);
        addTableColumnToMap(tableName, col);
        break;

      default:
        if(asn.getChildCount() > 1) {
          for(int i = 0; i < asn.getChildCount(); i++) {
            parseSelectExpr((ASTNode)asn.getChild(i));
          }
        }
    }

  }

  private void parseTokFunction(ASTNode ast, Set<String> set) {
    switch(ast.getType()) {
      case HiveParser.TOK_TABLE_OR_COL:
        String colRealName = convAliasToReal(columnAliasMap, ast.getChild(0).getText());
        set.add(colRealName);
        break;
      // Not only HiveParser.TOK_FUNCTION, but also HiveParser.KW_ADD, HiveParser.KW_WHEN ...
      default:
        for (int i = 0; i < ast.getChildCount(); i++) {
          ASTNode n = (ASTNode)ast.getChild(i);
          if (n != null) {
            parseTokFunction(n, set);
          }
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
      if (map != null && map.get(tb) != null) {
        Set<String> temp = map.get(tb);
        Set<String> value = new HashSet<>(temp);
        value.addAll(cols);
        map.put(tb, value);
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
