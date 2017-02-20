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

package org.apache.eagle.app.service.impl;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;

import java.io.PrintWriter;
import java.io.StringWriter;

public abstract class ApplicationHealthCheckBase extends HealthCheck {
    private static final String APP_ID_PATH = "appId";
    protected static final long DEFAULT_MAX_DELAY_TIME = 2 * 60 * 60 * 1000L;
    protected static final String MAX_DELAY_TIME_KEY = "application.maxDelayTime";

    protected Config config;

    @Inject
    private ApplicationEntityService applicationEntityService;

    protected ApplicationHealthCheckBase(Config config) {
        this.config = config;
    }

    protected ApplicationEntity.Status getApplicationStatus() {
        ApplicationEntity applicationEntity = applicationEntityService.getByUUIDOrAppId(null, config.getString(APP_ID_PATH));
        return applicationEntity.getStatus();
    }

    protected String printMessages(String ... messages) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        for (int i = 0; i < messages.length; i++) {
            pw.println(messages[i]);
        }
        return sw.getBuffer().toString();
    }

    protected String formatMillSeconds(long millseconds) {
        millseconds = millseconds / 1000;
        String result;
        if (millseconds <= 60) {
            result = millseconds + " seconds";
        } else if (millseconds > 60 && millseconds <= 3600) {
            result = String.format("%.2f minutes", millseconds * 1.0 / 60);
        } else if (millseconds > 3600 && millseconds <= 3600 * 24) {
            result = String.format("%.2f hours", millseconds * 1.0 / 3600);
        } else {
            result = String.format("%.2f days", millseconds * 1.0 / 3600 / 24);
        }

        return result;
    }
}
