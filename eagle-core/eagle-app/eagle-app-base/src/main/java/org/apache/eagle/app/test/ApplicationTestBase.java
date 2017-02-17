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
package org.apache.eagle.app.test;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.service.ApplicationStatusUpdateService;
import org.junit.Assert;
import org.junit.Before;

public class ApplicationTestBase {
    private Injector injector;


    @Inject
    ApplicationStatusUpdateService statusUpdateService;

    @Before
    public void setUp() {
        injector = Guice.createInjector(new ApplicationTestGuiceModule());
        injector.injectMembers(this);
    }

    protected Injector injector() {
        return injector;
    }

    protected void awaitApplicationStop(ApplicationEntity applicationEntity) throws InterruptedException {
        int attempt = 0;
        while (attempt < 30) {
            attempt ++;
            if (applicationEntity.getStatus() == ApplicationEntity.Status.STOPPED
                    || applicationEntity.getStatus() == ApplicationEntity.Status.INITIALIZED) {
                break;
            } else {
                statusUpdateService.updateApplicationEntityStatus(applicationEntity);
                Thread.sleep(1000);
            }
        }
        if (attempt >= 30) {
            Assert.fail("Failed to wait for application to STOPPED after 10 attempts");
        }
    }

    protected void awaitApplicationStatus(ApplicationEntity applicationEntity, ApplicationEntity.Status status) throws InterruptedException {
        int attempt = 0;
        while (attempt < 30) {
            attempt ++;
            if (applicationEntity.getStatus() == status) {
                break;
            } else {
                statusUpdateService.updateApplicationEntityStatus(applicationEntity);
                Thread.sleep(1000);
            }
        }
        if (attempt >= 30) {
            Assert.fail("Failed to wait for application to STOPPED after 10 attempts");
        }
    }
}