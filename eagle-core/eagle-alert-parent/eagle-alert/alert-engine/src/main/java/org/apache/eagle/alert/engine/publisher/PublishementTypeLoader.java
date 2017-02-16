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

package org.apache.eagle.alert.engine.publisher;

import org.apache.eagle.alert.engine.coordinator.PublishmentType;
import org.apache.eagle.common.utils.ReflectionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class PublishementTypeLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(PublishementTypeLoader.class);

    private final List<PublishmentType> publishmentTypeSet;

    private PublishementTypeLoader() {
        this.publishmentTypeSet = new LinkedList<>();
        LOGGER.info("Loading alert publish plugins ...");
        for (Class<? extends AlertPublishPluginProvider> clazz: ReflectionsHelper.getInstance().getSubTypesOf(AlertPublishPluginProvider.class)) {
            LOGGER.debug("Loading alert publish plugin: {}", clazz);
            try {
                PublishmentType type = clazz.newInstance().getPluginType();
                this.publishmentTypeSet.add(type);
                LOGGER.info("Loaded alert publish plugin {}:{}", type.getName(), type.getType());
            } catch (InstantiationException | IllegalAccessException e) {
                LOGGER.error("Failed to get instantiate alert publish plugin provider: {}", clazz, e);
            }
        }
        LOGGER.info("Loaded {} alert publish plugins", this.publishmentTypeSet.size());
    }

    private static final PublishementTypeLoader INSTANCE = new PublishementTypeLoader();

    public static List<PublishmentType> loadPublishmentTypes() {
        return INSTANCE.getPublishmentTypes();
    }

    public List<PublishmentType> getPublishmentTypes() {
        return publishmentTypeSet;
    }
}