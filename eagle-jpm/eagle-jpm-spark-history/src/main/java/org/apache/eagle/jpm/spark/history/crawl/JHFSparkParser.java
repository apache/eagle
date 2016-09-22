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

package org.apache.eagle.jpm.spark.history.crawl;


import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class JHFSparkParser implements JHFParserBase {

    private static final Logger logger = LoggerFactory.getLogger(JHFSparkParser.class);

    private boolean isValidJson;

    private JHFSparkEventReader eventReader;

    public JHFSparkParser(JHFSparkEventReader reader) {
        this.eventReader = reader;
    }

    @Override
    public void parse(InputStream is) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                isValidJson = true;
                JSONObject eventObj = parseAndValidateJSON(line);
                if (isValidJson) {
                    this.eventReader.read(eventObj);
                }
            }
            this.eventReader.clearReader();
        }
    }

    private JSONObject parseAndValidateJSON(String line) {
        JSONObject eventObj = null;
        JSONParser parser = new JSONParser();
        try {
            eventObj = (JSONObject) parser.parse(line);
        } catch (ParseException ex) {
            isValidJson = false;
            logger.error(String.format("Invalid json string. Fail to parse %s.", line), ex);
        }
        return eventObj;
    }
}