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

package org.apache.eagle.alert.engine.spout;

import java.util.Map;

import backtype.storm.spout.Scheme;


/**
 * All Scheme implementations should have the following conditions
 * 1) implement Scheme interface
 * 2) has one constructor with topic name as parameter
 */
public class SchemeBuilder {

    @SuppressWarnings("rawtypes")
    public static Scheme buildFromClsName(String clsName, String topic, Map conf) throws Exception{
        Object o = Class.forName(clsName).getConstructor(String.class, Map.class).newInstance(topic, conf);
        return (Scheme)o;
    }
}
