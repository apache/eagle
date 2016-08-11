/*
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
package org.apache.eagle.app.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

import java.util.Map;

//public class ApplicationConfigHelper {
//    private static final ObjectMapper mapper = new ObjectMapper();
//    public static <Conf extends Configuration> Conf convertFrom(Map<String,Object> configMap, Class<Conf> confClass){
//        return mapper.convertValue(configMap,confClass);
//    }
//
//    /**
//     *  Map application configuration from environment
//     *
//     * @param config
//     * @return
//     */
//    public static Map<String,Object> unwrapFrom(Config config, String namespace){
//        if(config.hasPath(namespace)) {
//            return config.getConfig(namespace).root().unwrapped();
//        }else{
//            Map<String,Object> unwrappedConfig = config.root().unwrapped();
//            if(unwrappedConfig.containsKey(namespace)){
//                return (Map<String,Object>) unwrappedConfig.get(namespace);
//            }else {
//                throw new IllegalArgumentException("Failed to load app config as config key: '"+namespace+"' was not found in: "+config);
//            }
//        }
//    }
//}