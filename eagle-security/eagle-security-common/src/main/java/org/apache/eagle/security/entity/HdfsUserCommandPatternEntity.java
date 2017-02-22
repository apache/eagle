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

package org.apache.eagle.security.entity;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;

import java.util.Map;

/**
 * UserPrincipal command pattern entity to specify Siddhi pattern, field selector and field modifier
 */
@Table("hdfsusercommandpattern")
@ColumnFamily("f")
@Prefix("hdfsusercommandpattern")
@Service(HdfsUserCommandPatternEntity.HDFS_USER_COMMAND_PATTERN_SERVICE)
@TimeSeries(false)
@Tags({"userCommand"})
public class HdfsUserCommandPatternEntity extends TaggedLogAPIEntity {
    public static final String HDFS_USER_COMMAND_PATTERN_SERVICE = "HdfsUserCommandPatternService";
    @Column("a")
    private String description;
    public String getDescription(){
        return description;
    }
    public void setDescription(String description){
        this.description = description;
    }

    @Column("b")
    private String pattern;

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
        valueChanged("pattern");
    }

    @Column("c")
    private Map<String, String> fieldSelector;

    public Map<String, String> getFieldSelector(){
        return fieldSelector;
    }

    public void setFieldSelector(Map<String, String> fieldSelector){
        this.fieldSelector = fieldSelector;
        valueChanged("fieldSelector");
    }

    @Column("d")
    private Map<String, String> fieldModifier;

    public Map<String, String> getFieldModifier(){
        return fieldModifier;
    }

    public void setFieldModifier(Map<String, String> fieldModifier){
        this.fieldModifier = fieldModifier;
        valueChanged("fieldModifier");
    }
}
