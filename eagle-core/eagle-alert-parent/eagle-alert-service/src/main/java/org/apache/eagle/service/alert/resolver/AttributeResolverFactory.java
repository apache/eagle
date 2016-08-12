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
package org.apache.eagle.service.alert.resolver;

import com.typesafe.config.Config;
import org.apache.eagle.metadata.service.ApplicationEntityService;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
* @since 6/16/15
*/
public final class AttributeResolverFactory {
    private final static Map<String,AttributeResolvable> fieldResolvableCache = Collections.synchronizedMap(new HashMap<>());
    public static AttributeResolvable getAttributeResolver(String fieldResolverName,
                                                           ApplicationEntityService entityService,
                                                           Config eagleServerConfig) throws AttributeResolveException {
        AttributeResolvable instance;
        if(fieldResolvableCache.containsKey(fieldResolverName)){
            instance = fieldResolvableCache.get(fieldResolverName);
        } else {
            try {
                instance = (AttributeResolvable) Class.forName(fieldResolverName).
                        getConstructor(ApplicationEntityService.class, Config.class).
                        newInstance(entityService, eagleServerConfig);
                fieldResolvableCache.put(fieldResolverName, instance);
            } catch (ClassNotFoundException e) {
                throw new AttributeResolveException("Attribute Resolver in type of "+fieldResolverName+" is not found",e);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new AttributeResolveException(e);
            } catch (Exception ex){
                throw new AttributeResolveException(ex);
            }
        }
        return instance;
    }
}