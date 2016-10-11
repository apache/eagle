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

import org.apache.eagle.app.example.extensions.ExampleCommonService;
import org.apache.eagle.app.example.extensions.ExampleCommonServiceImpl;
import org.apache.eagle.app.example.extensions.ExampleEntityService;
import org.apache.eagle.app.example.extensions.ExampleEntityServiceMemoryImpl;
import org.apache.eagle.app.service.ApplicationListener;
import org.apache.eagle.app.spi.AbstractApplicationProvider;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Define application provider pragmatically
 */
public class ExampleApplicationProvider extends AbstractApplicationProvider<ExampleStormApplication> {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleApplicationProvider.class);

    @Override
    public ExampleStormApplication getApplication() {
        return new ExampleStormApplication();
    }

    @Override
    public ApplicationListener getApplicationListener() {
        return new ApplicationListener() {
            private ApplicationEntity application;

            @Override
            public void init(ApplicationEntity application) {
                this.application = application;
                LOG.info("init {}",this.application);
            }

            @Override
            public void onInstall() {
                LOG.info("onInstall {}",this.application);
            }

            @Override
            public void onUninstall() {
                LOG.info("onUninstall {}",this.application);
            }

            @Override
            public void onStart() {
                LOG.info("onStart {}",this.application);
            }

            @Override
            public void onStop() {
                LOG.info("onStop {}",this.application);
            }
        };
    }

    @Override
    protected void onRegister() {
        bindToMemoryMetaStore(ExampleEntityService.class,ExampleEntityServiceMemoryImpl.class);
        bind(ExampleCommonService.class,ExampleCommonServiceImpl.class);
    }
}