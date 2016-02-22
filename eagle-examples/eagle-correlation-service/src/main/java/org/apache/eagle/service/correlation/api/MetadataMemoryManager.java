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
package org.apache.eagle.service.correlation.api;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by yonzhang on 2/20/16.
 */
public class MetadataMemoryManager<T> {
    private static MetadataMemoryManager instance = new MetadataMemoryManager();
    private ArrayList<T> metrics = new ArrayList<T>();
    private HashMap<T, ArrayList<T> > groups = new HashMap<T, ArrayList<T> >();

    private MetadataMemoryManager() {

    }

    public static MetadataMemoryManager getInstance() {
        return instance;
    }

    public  HashMap<T, ArrayList<T> > findAllGroups() {
        return groups;
    }

    public ArrayList<T> findAllMetrics() {
        return metrics;
    }

    public boolean addMetric(T id){
        T m = id;
        if(metrics.add(m)){
            return true;
        }
        else
            return false;
    }
    public boolean addGroup(T id, ArrayList<T> list){
    	groups.put(id, list);
    	return true;
    }
    
    public boolean checkMetric(T id){
    	return metrics.contains(id);
    		
    }
    
    public boolean checkGroup(T id){
    	return groups.containsKey(id);
    }
}
