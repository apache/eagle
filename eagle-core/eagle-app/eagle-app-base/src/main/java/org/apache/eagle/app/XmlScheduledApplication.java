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

package org.apache.eagle.app;

import com.typesafe.config.Config;
import org.apache.eagle.app.environment.impl.AbstractSchedulingPlan;
import org.apache.eagle.app.environment.impl.ScheduledEnvironment;
import org.apache.eagle.app.environment.impl.SchedulingPlan;
import org.quartz.SchedulerException;

/**
 * TODO: Support load scheduling from XML instead of in-line code.
 */
public class XmlScheduledApplication extends ScheduledApplication {
    private final String schedulingXmlFile;

    public XmlScheduledApplication(String schedulingXmlFile) {
        this.schedulingXmlFile = schedulingXmlFile;
    }

    @Override
    public SchedulingPlan execute(Config config, ScheduledEnvironment environment) {
        return new XmlSchedulingPlan(schedulingXmlFile, config, environment);
    }

    private class XmlSchedulingPlan extends AbstractSchedulingPlan {
        public XmlSchedulingPlan(String schedulingXmlFile, Config config, ScheduledEnvironment environment) {
            super(config, environment);
            throw new RuntimeException("Just proposal, not implemented yet");
        }

        @Override
        public String getAppId() {
            return null;
        }

        @Override
        public void schedule() throws SchedulerException {

        }

        @Override
        public boolean unschedule() throws SchedulerException {
            return false;
        }

        @Override
        public boolean scheduling() throws SchedulerException {
            return false;
        }
    }
}