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
package org.apache.eagle.app.base;

import org.apache.eagle.app.base.metadata.ApplicationInstance;
import org.apache.eagle.app.base.metadata.ApplicationSpec;
import org.apache.eagle.app.base.metadata.ApplicationsConfig;

import java.util.List;

public abstract class ApplicationManager {
    /**
     * Load applications
     */
    public abstract void load();

    /**
     * Reload applications
     */
    public void reload(){
        load();
    }

    /**
     * Get all available applications
     * @return
     */
    public abstract List<ApplicationSpec> getAllApplications();

    /**
     *
     * @param instance
     */
    public abstract void startApp(ApplicationInstance instance);

    /**
     *
     * @param instance
     */
    public abstract void stopApp(ApplicationInstance instance);

    /**
     *
     * @param instance
     */
    public abstract void installApp(ApplicationInstance instance);

    /**
     *
     * @param instance
     */
    public abstract void uninstallApp(ApplicationInstance instance);

    private static ApplicationManager instance;

    public static ApplicationManager getInstance(){
        if(instance == null){
            instance = new ApplicationManagerImpl();
            instance.load();
        }
        return instance;
    }

    public abstract ApplicationSpec getApplicationByType(String appType);
}