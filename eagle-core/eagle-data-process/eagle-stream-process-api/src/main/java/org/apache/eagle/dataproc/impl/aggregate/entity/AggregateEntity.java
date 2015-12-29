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
package org.apache.eagle.dataproc.impl.aggregate.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Event entity during stream processing
 * 
 * @since Dec 17, 2015
 *
 */
public class AggregateEntity implements Serializable {

	private static final long serialVersionUID = 5911351515190098292L;

    private Map<String, Object> results = new HashMap<>();

    private List<Object> result = new LinkedList<>();

    public void add(String col, Object res) {
        results.put(col, res);
    }

    public void add(Object res) {
        result.add(res);
    }
	
}
