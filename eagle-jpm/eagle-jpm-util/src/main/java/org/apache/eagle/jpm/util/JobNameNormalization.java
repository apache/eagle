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

package org.apache.eagle.jpm.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JobNameNormalization {
    private static Logger LOG = LoggerFactory.getLogger(JobNameNormalization.class);
    private static final String JOB_NAME_NORMALIZATION_RULES_KEY = "job.name.normalization.rules.key";
    private static final String PARAMETERIZED_PREFIX = "\\$";
    private static final String MULTIPLE_RULE_DILIMITER = ";";
    /**
     * map from source string to target string.
     * source string is regular expression, for example ^(.*)[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}$
     * target string is parameterized string, for example $1, $2
     */
    private List<JobNameNormalizationRule> rules = new ArrayList<JobNameNormalizationRule>();

    private enum NormalizationOp {
        REPLACE("=>");
        private String value;

        private NormalizationOp(String value) {
            this.value = value;
        }

        public String toString() {
            return value;
        }
    }

    static class JobNameNormalizationRule {
        Pattern pattern;
        NormalizationOp op;
        String target;
    }

    private JobNameNormalization(Config conf) {
        try {
            // load normalization rules
            String key = JOB_NAME_NORMALIZATION_RULES_KEY.toLowerCase();
            String value = conf.getString(key) != null ? conf.getString(key) : Constants.JOB_NAME_NORMALIZATION_RULES_KEY_DEFAULT;
            // multiple rules are concatenated with semicolon, i.e. ;
            String[] rules = value.split(MULTIPLE_RULE_DILIMITER);
            for (String rule : rules) {
                rule = rule.trim();
                LOG.info("jobNormalizationRule is loaded " + rule);
                addRule(rule);
            }
        } catch (Exception ex) {
            LOG.error("fail loading job name normalization rules", ex);
            throw new RuntimeException(ex);
        }
    }

    public static JobNameNormalization getInstance(Config config) {
        return new JobNameNormalization(config);
    }

    private void addRule(String rule) {
        for (NormalizationOp op : NormalizationOp.values()) {
            // split the rule to be source and target string
            String[] elements = rule.split(op.toString());
            if (elements == null || elements.length != 2) {
                return;
            }
            JobNameNormalizationRule r = new JobNameNormalizationRule();
            r.pattern = Pattern.compile(elements[0].trim());
            r.op = op;
            r.target = elements[1].trim();
            rules.add(r);
            break;  //once one Op is matched, exit
        }

    }

    public String normalize(String jobName) {
        String normalizedJobName = jobName;
        // go through each rules and do actions
        for (JobNameNormalizationRule rule : rules) {
            Pattern p = rule.pattern;
            Matcher m = p.matcher(jobName);
            if (m.find()) {
                normalizedJobName = rule.target;
                int c = m.groupCount();
                for (int i = 1; i < c + 1; i++) {
                    normalizedJobName = normalizedJobName.replaceAll(PARAMETERIZED_PREFIX + String.valueOf(i), m.group(i));
                }
            }
        }
        return normalizedJobName;
    }
}
