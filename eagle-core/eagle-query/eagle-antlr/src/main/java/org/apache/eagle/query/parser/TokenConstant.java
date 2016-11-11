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
package org.apache.eagle.query.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TokenConstant {
    public static final Pattern EXP_PATTERN = Pattern.compile("^EXP\\{(.+)\\}(\\s+AS\\s+)?\\s*(.+)?\\s*$", Pattern.CASE_INSENSITIVE);
    public static final Pattern STRING_PATTERN = Pattern.compile("^(\"(.*?\n)*.*\")$");
    public static final Pattern ARRAY_PATTERN = Pattern.compile("^(\\(.*\\))$");
    public static final Pattern NUMBER_PATTERN = Pattern.compile("^((-|\\+)?\\s*[0-9]+(\\.[0-9]+)?)$");
    public static final Pattern NULL_PATTERN = Pattern.compile("^(NULL|null)$");
    public static final Pattern ID_PATTERN = Pattern.compile("^@(.+)$");

    public static final String ID_PREFIX = "@";
    public static final String WHITE_SPACE = "";

    public static boolean isExpression(String query) {
        if (query == null) {
            return false;
        }
        Matcher matcher = EXP_PATTERN.matcher(query);
        return matcher.matches();
    }

    /**
     * EXP{ expression } AS alias  => expression.
     *
     * @param expression
     * @return
     */
    public static String parseExpressionContent(String expression) {
        Matcher matcher = EXP_PATTERN.matcher(expression);
        if (matcher.find()) {
            expression = matcher.group(1);
        }
        return expression;
    }
}