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
package org.apache.eagle.app.tools;

import com.typesafe.config.Config;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.ApplicationContext;
import org.apache.eagle.metadata.model.ApplicationEntity;

public abstract class ApplicationTester {
    private static ApplicationTester instance;
    public static ApplicationTester getInstance(){
        if(instance == null){
            instance = new ApplicationTesterImpl();
        }
        return instance;
    }

    public abstract void run(Application application,
                             ApplicationEntity.Mode mode,
                             String appId,
                             Config config);

    private static class ApplicationTesterImpl extends ApplicationTester{
        @Override
        public void run(Application application,
                        ApplicationEntity.Mode mode,String appId,Config config) {
            ApplicationEntity applicationEntity = new ApplicationEntity();
            applicationEntity.setAppId(appId);
            applicationEntity.setMode(mode);
            ApplicationContext context = new ApplicationContext(applicationEntity,config);
            application.start(context);
        }
    }
}