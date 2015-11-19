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
package org.apache.eagle.service.hbase;

import java.util.ArrayList;
import java.util.List;

public class Tables {
    List<String> tables = new ArrayList<String>();
    public Tables(){
        tables.add("eagle_metric");

        tables.add("actiondetail");
        tables.add("alertdetail");
        tables.add("alertgroup");
        tables.add("alertmeta");
        tables.add("alertMetaEntity");

        // for alert framework
        tables.add("alertDataSource");
        tables.add("alertStream");
        tables.add("alertExecutor");
        tables.add("alertStreamSchema");
        tables.add("alertdef");

        // for security
        tables.add("hiveResourceSensitivity");
        tables.add("fileSensitivity");
        tables.add("mlmodel");
        tables.add("userprofile");
    }

    public List<String> getTables(){
        return this.tables;
    }
}
