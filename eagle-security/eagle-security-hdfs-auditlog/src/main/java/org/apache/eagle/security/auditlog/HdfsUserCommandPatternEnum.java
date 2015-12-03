/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.security.auditlog;

import java.util.SortedMap;

/**
 * Created by yonzhang on 11/24/15.
 */
public enum HdfsUserCommandPatternEnum {
    APPEND("append", HdfsUserCommandPatternsConstant.APPEND_PATTERN, HdfsUserCommandPatternsConstant.APPEND_SIDDHI_OUTPUT_SELECTOR, HdfsUserCommandPatternsConstant.APPEND_OUTPUT_MODIFIER),
    READ("read", HdfsUserCommandPatternsConstant.READ_PATTERN, HdfsUserCommandPatternsConstant.READ_SIDDHI_OUTPUT_SELECTOR, HdfsUserCommandPatternsConstant.READ_OUTPUT_MODIFIER),
    COPYFROMLOCAL("copyFromLocal", HdfsUserCommandPatternsConstant.COPYFROMLOCAL_PATTERN, HdfsUserCommandPatternsConstant.COPYFROMLOCAL_SIDDHI_OUTPUT_SELECTOR, HdfsUserCommandPatternsConstant.COPYFROMLOCAL_OUTPUT_MODIFIER);

    private HdfsUserCommandPatternEnum(String userCommand, String pattern, SortedMap<String, String> outputSelector, SortedMap<String, String> outputModifier){
        this.userCommand = userCommand;
        this.pattern = pattern;
        this.outputSelector = outputSelector;
        this.outputModifier = outputModifier;
    }

    public String getUserCommand(){
        return userCommand;
    }
    public String getPattern(){
        return pattern;
    }
    public SortedMap<String, String> getOutputSelector(){
        return outputSelector;
    }
    public SortedMap<String, String> getOutputModifier(){
        return outputModifier;
    }
    private String userCommand;
    private String pattern;
    private SortedMap<String, String> outputSelector;
    private SortedMap<String, String> outputModifier;
}
