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
package org.apache.eagle.security.hive.jobrunning;

import com.typesafe.config.Config;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.apache.eagle.datastream.Tuple2;
import org.apache.eagle.security.hive.ql.HiveQLParserContent;
import org.apache.eagle.security.hive.ql.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * parse hive query log
 */
public class HiveQueryParserExecutor extends JavaStormStreamExecutor2<String, Map> {
	private static final long serialVersionUID = -5878930561335302957L;
	private static final Logger LOG = LoggerFactory.getLogger(HiveQueryParserExecutor.class);

	private Config config;

    @Override
	public void prepareConfig(Config config) {
		this.config = config;
	}

    @Override
	public void init(){

	}

    @Override
    public void flatMap(java.util.List<Object> input, Collector<Tuple2<String, Map>> outputCollector){
        /**
         * hiveQueryLog includes the following key value pair
         * "hive.current.database" -> <database name>
         * "hive.query.string" -> <hive query statement>
         * "mapreduce.job.user.name" -> <user name>
         * TODO we need hive job start and end time
         */
        String user = (String)input.get(0);
        @SuppressWarnings("unchecked")
        Map<String, Object> hiveQueryLog = (Map<String, Object>)input.get(1);
        LOG.info("Receive hive query log: " + hiveQueryLog);

        String query = null;
        String db = null;
        String userName = null;
        long timestamp = -1;
        for (Entry<String, Object> entry : hiveQueryLog.entrySet()) {
            switch (entry.getKey()) {
                case "hive.query.string":
                    if (entry.getValue() != null) {
                        query = entry.getValue().toString();
                    }
                    break;
                case "hive.current.database":
                    if (entry.getValue() != null) {
                        db = entry.getValue().toString();
                    }
                    break;
                case "mapreduce.job.user.name":
                    if (entry.getValue() != null) {
                        userName = entry.getValue().toString();
                    }
                    break;
                case "mapreduce.job.cache.files.timestamps":
                    if (entry.getValue() != null) {
                        String timestampString = (String) entry.getValue();
                        String[] timestampArray = timestampString.split("\\s*,\\s*");
              /* Get timestamp of start time. */
                        timestamp = Long.parseLong(timestampArray[0]);
                    }
                    break;
            }
        }

        HiveQLParserContent parserContent;
        Parser queryParser = new Parser();
        try {
            parserContent = queryParser.run(query);
        } catch (Exception ex) {
            LOG.error("Failed running hive query parser.", ex);
            throw new IllegalStateException(ex);
        }

        /**
         * Generate "resource" field: /db/table/column
         * "resource" -> </db/table/column1,/db/table/column2,...>
         */
        StringBuilder resources = new StringBuilder();
        String prefix = ",";
        String connector = "/";
        for (Entry<String, Set<String>> entry
                : parserContent.getTableColumnMap().entrySet()) {
            String table = entry.getKey();
            Set<String> colSet = entry.getValue();
            /**
             * If colSet is empty, it means no column is accessed in the table.
             * So column is not added to the event stream.
             * Only /db/table
             */
            if (colSet.isEmpty()) {
                resources.append(connector).append(db).append(connector).append(table).append(prefix);
            } else {
                for (String col : colSet) {
                    resources.append(connector).append(db).append(connector).append(table);
                    if (col != null && col.length() > 0) {
                        resources.append(connector).append(col);
                    }
                    resources.append(prefix);
                }
            }
        }
        /* Remove the last prefix: "," */
        resources.setLength(resources.length() - 1);

        /* <event> has to be SortedMap. */
        Map<String, Object> event = new TreeMap<String, Object>();
        event.put("user", userName);
        event.put("command", parserContent.getOperation());
        event.put("timestamp", timestamp);
        event.put("resource", resources.toString());
        LOG.info("HiveQL Parser event stream. " + event);

        outputCollector.collect(new Tuple2(user, event));
    }
}