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

package org.apache.eagle.app.config;

import org.apache.eagle.app.spi.ApplicationProvider;

public class ApplicationProviderConfig {
    private String jarPath;
    private String className;

    public ApplicationProviderConfig(){}
    public ApplicationProviderConfig(String jarPath, Class<? extends ApplicationProvider> className){
        this.jarPath = jarPath;
        this.className = className.getCanonicalName();
    }
    public ApplicationProviderConfig(String jarPath, String className){
        this.jarPath = jarPath;
        this.className = className;
    }
    @Override
    public String toString() {
        return String.format("ApplicationProviderConfig[jarPath=%s,className=%s]",this.getJarPath(),this.getClassName());
    }

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }
}