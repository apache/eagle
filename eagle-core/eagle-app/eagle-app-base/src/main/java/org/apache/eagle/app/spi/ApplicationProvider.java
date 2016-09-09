/**
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

import org.apache.eagle.app.Application;
import org.apache.eagle.app.config.ApplicationProviderConfig;
import org.apache.eagle.common.module.ModuleRegistry;
import org.apache.eagle.metadata.model.ApplicationDesc;
import com.typesafe.config.Config;

public interface ApplicationProvider<T extends Application> {

    void prepare(ApplicationProviderConfig providerConfig,Config envConfig);

    /**
     * @return application descriptor.
     */
    ApplicationDesc getApplicationDesc();

    /**
     * @return application instance.
     */
    T getApplication();

    void register(ModuleRegistry registry);
}