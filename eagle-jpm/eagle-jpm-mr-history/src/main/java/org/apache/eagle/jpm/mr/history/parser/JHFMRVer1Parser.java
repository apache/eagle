/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.mr.history.parser;

import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class JHFMRVer1Parser implements JHFParserBase {
    private static final Logger logger = LoggerFactory.getLogger(JHFMRVer1Parser.class);
    static final char LINE_DELIMITER_CHAR = '.';
    static final char[] charsToEscape = new char[] {'"', '=', LINE_DELIMITER_CHAR};
    static final String KEY = "(\\w+)";
    // value is any character other than quote, but escaped quotes can be there
    static final String VALUE = "[^\"\\\\]*+(?:\\\\.[^\"\\\\]*+)*+";
    static final Pattern pattern = Pattern.compile(KEY + "=" + "\"" + VALUE + "\"");
    static final String MAX_COUNTER_COUNT = "10000";

    private JHFMRVer1EventReader reader;

    public JHFMRVer1Parser(JHFMRVer1EventReader reader) {
        this.reader = reader;
    }

    /**
     * Parses history file and invokes Listener.handle() for
     * each line of history. It can be used for looking through history
     * files for specific items without having to keep whole history in memory.
     *
     * @throws IOException
     */
    @Override
    public void parse(InputStream in) throws Exception, ParseException {
        // set enough counter number as user may build more counters
        System.setProperty("mapreduce.job.counters.max", MAX_COUNTER_COUNT);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        try {
            String line = null;
            StringBuffer buf = new StringBuffer();

            // Read the meta-info line. Note that this might be a jobinfo line for files written with older format
            line = reader.readLine();

            // Check if the file is empty
            if (line == null) {
                return;
            }

            // Get the information required for further processing
            MetaInfoManager mgr = new MetaInfoManager(line);
            boolean isEscaped = mgr.isValueEscaped();
            String lineDelim = String.valueOf(mgr.getLineDelim());
            String escapedLineDelim = StringUtils.escapeString(lineDelim, StringUtils.ESCAPE_CHAR, mgr.getLineDelim());

            do {
                buf.append(line);
                if (!line.trim().endsWith(lineDelim) || line.trim().endsWith(escapedLineDelim)) {
                    buf.append("\n");
                    continue;
                }
                parseLine(buf.toString(), this.reader, isEscaped);
                buf = new StringBuffer();
            }
            while ((line = reader.readLine()) != null);

            // flush to tell listener that we have finished parsing
            logger.info("finish parsing job history file and close");
            this.reader.close();
        } catch (Exception ex) {
            logger.error("can not parse correctly ", ex);
            throw ex;
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    private static void parseLine(String line, JHFMRVer1PerLineListener l, boolean isEscaped) throws Exception, ParseException {
        // extract the record type
        int idx = line.indexOf(' ');
        String recType = line.substring(0, idx);
        String data = line.substring(idx + 1, line.length());

        Matcher matcher = pattern.matcher(data);
        Map<Keys, String> parseBuffer = new HashMap<Keys, String>();

        while (matcher.find()) {
            String tuple = matcher.group(0);
            String[] parts = StringUtils.split(tuple, StringUtils.ESCAPE_CHAR, '=');
            String value = parts[1].substring(1, parts[1].length() - 1);
            if (isEscaped) {
                value = StringUtils.unEscapeString(value, StringUtils.ESCAPE_CHAR, charsToEscape);
            }
            parseBuffer.put(Keys.valueOf(parts[0]), value);
        }

        try {
            l.handle(RecordTypes.valueOf(recType), parseBuffer);
        } catch (IllegalArgumentException ex) {
            // somehow record type does not exist, but continue to run
            logger.warn("record type does not exist " + recType, ex);
        }

        parseBuffer.clear();
    }

    /**
     * Manages job-history's meta information such as version etc.
     * Helps in logging version information to the job-history and recover
     * version information from the history.
     */
    static class MetaInfoManager implements JHFMRVer1PerLineListener {
        private long version = 0L;
        private KeyValuePair pairs = new KeyValuePair();

        public void close() {
        }

        // Extract the version of the history that was used to write the history
        public MetaInfoManager(String line) throws Exception, ParseException {
            if (null != line) {
                // Parse the line
                parseLine(line, this, false);
            }
        }

        // Get the line delimiter
        char getLineDelim() {
            if (version == 0) {
                return '"';
            } else {
                return LINE_DELIMITER_CHAR;
            }
        }

        // Checks if the values are escaped or not
        boolean isValueEscaped() {
            // Note that the values are not escaped in version 0
            return version != 0;
        }

        public void handle(RecordTypes recType, Map<Keys, String> values) throws IOException {
            // Check if the record is of type META
            if (RecordTypes.Meta == recType) {
                pairs.handle(values);
                version = pairs.getLong(Keys.VERSION); // defaults to 0
            }
        }
    }

    /**
     * Base class contais utility stuff to manage types key value pairs with enums.
     */
    static class KeyValuePair {
        private Map<Keys, String> values = new HashMap<Keys, String>();

        /**
         * Get 'String' value for given key. Most of the places use Strings as
         * values so the default get' method returns 'String'.  This method never returns
         * null to ease on GUIs. if no value is found it returns empty string ""
         *
         * @param k
         * @return if null it returns empty string - ""
         */
        public String get(Keys k) {
            String s = values.get(k);
            return s == null ? "" : s;
        }

        /**
         * Convert value from history to int and return.
         * if no value is found it returns 0.
         *
         * @param k key
         */
        public int getInt(Keys k) {
            String s = values.get(k);
            if (null != s) {
                return Integer.parseInt(s);
            }
            return 0;
        }

        /**
         * Convert value from history to int and return.
         * if no value is found it returns 0.
         *
         * @param k
         */
        public long getLong(Keys k) {
            String s = values.get(k);
            if (null != s) {
                return Long.parseLong(s);
            }
            return 0;
        }

        /**
         * Set value for the key.
         *
         * @param k
         * @param s
         */
        public void set(Keys k, String s) {
            values.put(k, s);
        }

        /**
         * Adds all values in the Map argument to its own values.
         *
         * @param m
         */
        public void set(Map<Keys, String> m) {
            values.putAll(m);
        }

        /**
         * Reads values back from the history, input is same Map as passed to Listener by parseHistory().
         *
         * @param values
         */
        public synchronized void handle(Map<Keys, String> values) {
            set(values);
        }

        /**
         * Returns Map containing all key-values.
         */
        public Map<Keys, String> getValues() {
            return values;
        }
    }

    /**
     * Job history files contain key="value" pairs, where keys belong to this enum.
     * It acts as a global namespace for all keys.
     */
    public static enum Keys {
        JOBTRACKERID,
        START_TIME, FINISH_TIME, JOBID, JOBNAME, USER, JOBCONF, SUBMIT_TIME,
        LAUNCH_TIME, TOTAL_MAPS, TOTAL_REDUCES, FAILED_MAPS, FAILED_REDUCES,
        FINISHED_MAPS, FINISHED_REDUCES, JOB_STATUS, TASKID, HOSTNAME, TASK_TYPE,
        ERROR, TASK_ATTEMPT_ID, TASK_STATUS, COPY_PHASE, SORT_PHASE, REDUCE_PHASE,
        SHUFFLE_FINISHED, SORT_FINISHED, COUNTERS, SPLITS, JOB_PRIORITY, HTTP_PORT,
        TRACKER_NAME, STATE_STRING, VERSION, MAP_COUNTERS, REDUCE_COUNTERS,
        VIEW_JOB, MODIFY_JOB, JOB_QUEUE, RACK,

        UBERISED, SPLIT_LOCATIONS, FAILED_DUE_TO_ATTEMPT, MAP_FINISH_TIME, PORT, RACK_NAME,

        //For Artemis
        WORKFLOW_ID, WORKFLOW_NAME, WORKFLOW_NODE_NAME, WORKFLOW_ADJACENCIES, WORKFLOW_TAGS,
        SHUFFLE_PORT, LOCALITY, AVATAAR, FAIL_REASON
    }
}

