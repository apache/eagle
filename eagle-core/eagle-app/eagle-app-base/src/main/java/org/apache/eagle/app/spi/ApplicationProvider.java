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

package org.apache.eagle.app.spi;

import com.codahale.metrics.health.HealthCheck;
import com.typesafe.config.Config;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.service.ApplicationListener;
import org.apache.eagle.common.module.ModuleRegistry;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.service.ApplicationEntityService;

import java.lang.reflect.ParameterizedType;
import java.util.Optional;

/**
 * Application Service Provider Interface.
 *
 * @param <T> Application Type.
 */
public interface ApplicationProvider<T extends Application> {

    /**
     * @return application descriptor.
     */
    ApplicationDesc getApplicationDesc();

    /**
     * Get Application Instance Type, by default load from generic parameter automatically
     *
     * @return application class type if exists.
     */
    default Class<T> getApplicationClass() {
        try {
            String className = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0].getTypeName();
            Class<?> clazz = Class.forName(className);
            return (Class<T>) clazz;
        } catch (Exception e) {
            throw new IllegalStateException(
                "Unable to get generic application class, "
                    + "reason: class is not parametrized with generic type, "
                    + "please provide application class by overriding getApplicationClass()");
        }
    }

    /**
     * @return application instance.
     */
    T getApplication();

    /**
     * @return application lifecycle listeners type.
     */
    Optional<ApplicationListener> getApplicationListener();

    /**
     * Extend application modules like Web Resource, Metadata Store, etc.
     */
    void register(ModuleRegistry registry);

    default HealthCheck getAppHealthCheck(Config config) {
        return new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return Result.healthy();
            }
        };
    }
}