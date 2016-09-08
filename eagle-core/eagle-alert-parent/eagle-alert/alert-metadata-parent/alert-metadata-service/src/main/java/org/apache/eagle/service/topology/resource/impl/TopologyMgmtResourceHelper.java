/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.service.topology.resource.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

public class TopologyMgmtResourceHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TopologyMgmtResourceHelper.class);

    public static <T> Optional<T> findById(List<T> clusters, String id) {
        Optional<T> OptionValue = clusters.stream().filter(o -> getName(o).equalsIgnoreCase(id)).findFirst();
        return OptionValue;
    }

    public static <T> String getName(T t) {
        try {
            Method m = t.getClass().getMethod("getName");
            return (String) m.invoke(t);
        } catch (NoSuchMethodException | SecurityException | InvocationTargetException | IllegalAccessException
            | IllegalArgumentException e) {
            LOG.error(" getName not found on given class :" + t.getClass().getName());
        }
        throw new RuntimeException(String.format("no getName() found on target class %s for matching", t.getClass()
            .getName()));
    }
}
