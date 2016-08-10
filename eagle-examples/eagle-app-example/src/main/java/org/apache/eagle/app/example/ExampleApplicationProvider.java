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
package org.apache.eagle.app.example;

import com.google.inject.AbstractModule;
import org.apache.eagle.app.example.extensions.ExampleService;
import org.apache.eagle.app.example.extensions.ExampleServiceMemoryImpl;
import org.apache.eagle.app.spi.AbstractApplicationProvider;
import org.apache.eagle.common.module.ModuleRegistry;
import org.apache.eagle.metadata.service.memory.MemoryMetadataStore;

/**
 * Define application provider pragmatically
 */
public class ExampleApplicationProvider extends AbstractApplicationProvider<ExampleStormApplication> {
    @Override
    protected String getMetadata() {
        return "/META-INF/apps/example/metadata.xml";
    }

    @Override
    public ExampleStormApplication getApplication() {
        return new ExampleStormApplication();
    }

    @Override
    public void register(ModuleRegistry registry) {
        registry.register(MemoryMetadataStore.class, new AbstractModule() {
            @Override
            protected void configure() {
                bind(ExampleService.class).to(ExampleServiceMemoryImpl.class);
            }
        });
    }
}