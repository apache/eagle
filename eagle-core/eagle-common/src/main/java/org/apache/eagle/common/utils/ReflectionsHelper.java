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

package org.apache.eagle.common.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReflectionsHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReflectionsHelper.class);
    private final Reflections reflections;
    private static final String DEFAULT_PACKAGE = "org.apache.eagle";

    private ReflectionsHelper() {
        Config config = ConfigFactory.load();
        String[] packages;
        if (config.hasPath("scanPackages")) {
            packages = config.getString("scanPackages").split(",");
        } else {
            packages = new String[]{DEFAULT_PACKAGE};
        }
        LOGGER.info("Scanning packages: {}", packages);
        this.reflections = new Reflections(packages);
    }

    private static ReflectionsHelper INSTANCE = new ReflectionsHelper();

    public static Reflections getInstance() {
        return INSTANCE.reflections;
    }
}