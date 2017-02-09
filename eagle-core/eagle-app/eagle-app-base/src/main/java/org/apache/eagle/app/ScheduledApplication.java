/*
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
package org.apache.eagle.app;

import com.typesafe.config.Config;
import org.apache.eagle.app.environment.ExecutionRuntimeManager;
import org.apache.eagle.app.environment.impl.ScheduledEnvironment;
import org.apache.eagle.app.environment.impl.ScheduledExecutionRuntime;
import org.apache.eagle.app.environment.impl.SchedulingPlan;

public abstract class ScheduledApplication extends ExecutableApplication<ScheduledEnvironment, SchedulingPlan> {
    @Override
    public Class<? extends ScheduledEnvironment> getEnvironmentType() {
        return ScheduledEnvironment.class;
    }

    @Override
    public void run(Config config) {
        ScheduledExecutionRuntime runtime = (ScheduledExecutionRuntime) ExecutionRuntimeManager.getInstance().getRuntimeSingleton(getEnvironmentType(), config);
        try {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        runtime.stop();
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
            });
            runtime.start();
            runtime.start(this, config);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static XmlScheduledApplication buildFromXml(String schedulingXmlFile) {
        return new XmlScheduledApplication(schedulingXmlFile);
    }
}