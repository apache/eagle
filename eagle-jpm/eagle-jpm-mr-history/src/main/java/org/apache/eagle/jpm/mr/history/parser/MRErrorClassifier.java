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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class MRErrorClassifier {
    
    private final List<ErrorCategory> categories = new ArrayList<ErrorCategory>();

    public MRErrorClassifier(InputStream configStream) throws IOException {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(configStream));
        String line;
        while((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            int index = line.indexOf("=");
            if (index == -1) {
                throw new IllegalArgumentException("Invalid config, line: " + line);
            }
            final String key = line.substring(0, index).trim();
            final String value = line.substring(index + 1).trim();
            index = value.indexOf("|");
            if (index == -1) {
                throw new IllegalArgumentException("Invalid config, line: " + line);
            }
            final boolean needTransform = Boolean.valueOf(value.substring(0, index));
            final Pattern pattern = Pattern.compile(value.substring(index + 1));
            final ErrorCategory category = new ErrorCategory();
            category.setName(key);
            category.setNeedTransform(needTransform);
            category.setPattern(pattern);
            categories.add(category);
        }
    }
    
    public List<ErrorCategory> getErrorCategories() {
        return categories;
    }

    public String classifyError(String error) throws IOException {
        for (ErrorCategory category : categories) {
            final String result = category.classify(error);
            if (result != null) {
                return result;
            }
        }
        return "UNKNOWN";
    }
    
    public static class ErrorCategory {
        private String name;
        private Pattern pattern;
        private boolean needTransform;
        
        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public Pattern getPattern() {
            return pattern;
        }
        public void setPattern(Pattern pattern) {
            this.pattern = pattern;
        }
        public boolean isNeedTransform() {
            return needTransform;
        }
        public void setNeedTransform(boolean needTransform) {
            this.needTransform = needTransform;
        }
        
        public String classify(String error) {
            Matcher matcher = pattern.matcher(error);
            if (matcher.find()) {
                if (!needTransform) {
                    return name;
                } else {
                    return matcher.group(1);
                }
            }
            return null;
        }
    }
}
