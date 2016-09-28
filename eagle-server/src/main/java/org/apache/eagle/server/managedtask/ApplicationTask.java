/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.server.managedtask;

import com.google.common.util.concurrent.AbstractScheduledService;
import io.dropwizard.lifecycle.Managed;
import org.apache.eagle.app.service.impl.ApplicationStatusUpdateServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationTask implements Managed {
    private final static Logger LOG = LoggerFactory.getLogger(ApplicationStatusUpdateServiceImpl.class);
    private final AbstractScheduledService service;

    public ApplicationTask(AbstractScheduledService service){
        this.service = service;
    }

    @Override
    public void start() throws Exception {
        LOG.info("Application update task started:");
        service.startAsync().awaitRunning();
    }

    @Override
    public void stop() throws Exception {
        service.stopAsync().awaitTerminated();
    }
}