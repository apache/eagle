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
import java.util.Map;
import java.util.Set;

public class HiveQLParserContent {
  /** Query operation */
  private String operation;
  /** Table name of insert operation */
  private String insertTable;

  /** "t1" -> ["col1", "col2", "col3"] */
  public Map<String, Set<String>> tableColumnMap;
  
  public HiveQLParserContent() {
    tableColumnMap = new HashMap<String, Set<String>>();
  }

  public void setOperation(String opt) {
    this.operation = opt;
  }

  public String getOperation() {
    return this.operation;
  }

  public void setInsertTable(String table) {
    this.insertTable = table;
  }

  public String getInsertTable() {
    return this.insertTable;
  }

  public Map<String, Set<String>> getTableColumnMap() {
    return this.tableColumnMap;
  }
}
