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
package org.apache.eagle.alert.coordinator;

import java.util.*;

/**
 * Created on 10/1/16.
 */
public class ValidateState {

    private boolean isOk = false;

    private List<String> unusedDataSources = new ArrayList<>();
    private List<String> unusedStreams = new ArrayList<>();
    private List<String> unPublishedPolicies = new ArrayList<>();

    /*
     * Includes validation of extension class existence
     * Policy expression validation
     * Inter-Reference validation
     */
    private Map<String, List<String>> dataSourcesValidation = new HashMap<>();
    private Map<String, List<String>> streamsValidation = new HashMap<>();
    private Map<String, List<String>> policiesValidation = new HashMap<>();
    private Map<String, List<String>> publishmentValidation = new HashMap<>();
    private Map<String, List<String>> topoMetaValidation = new HashMap<>();

    public void appendUnusedDatasource(String ds) {
        unusedDataSources.add(ds);
    }

    public void appendUnusedStreams(String s) {
        unusedStreams.add(s);
    }

    public void appendUnPublishedPolicies(String s) {
        unPublishedPolicies.add(s);
    }

    public void appendDataSourceValidation(String name, String msg) {
        if (!dataSourcesValidation.containsKey(name)) {
            dataSourcesValidation.putIfAbsent(name, new LinkedList<>());
        }
        dataSourcesValidation.get(name).add(msg);
    }

    public void appendStreamValidation(String name, String msg) {
        if (!streamsValidation.containsKey(name)) {
            streamsValidation.putIfAbsent(name, new LinkedList<>());
        }
        streamsValidation.get(name).add(msg);
    }

    public void appendPolicyValidation(String name, String msg) {
        if (!policiesValidation.containsKey(name)) {
            policiesValidation.putIfAbsent(name, new LinkedList<>());
        }
        policiesValidation.get(name).add(msg);
    }

    public void appendPublishemtnValidation(String name, String msg) {
        if (!publishmentValidation.containsKey(name)) {
            publishmentValidation.putIfAbsent(name, new LinkedList<>());
        }
        publishmentValidation.get(name).add(msg);
    }

    public void appendTopoMetaValidation(String name, String msg) {
        if (!topoMetaValidation.containsKey(name)) {
            topoMetaValidation.putIfAbsent(name, new LinkedList<>());
        }
        topoMetaValidation.get(name).add(msg);
    }

}
