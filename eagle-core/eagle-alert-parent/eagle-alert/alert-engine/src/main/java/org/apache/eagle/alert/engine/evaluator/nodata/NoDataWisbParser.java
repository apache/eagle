/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.evaluator.nodata;

import java.util.List;
import java.util.Set;

/**
 * Since 6/29/16.
 */
public interface NoDataWisbParser {
    /**
     * parse policy definition and return WISB values for one or multiple fields
     * for example host and data center are 2 fields for no data alert, then WISB is a list of two values.
     *
     * @param args some information parsed from policy defintion
     * @return list of list of field values
     */
    Set<List<String>> parse(String[] args);
}
