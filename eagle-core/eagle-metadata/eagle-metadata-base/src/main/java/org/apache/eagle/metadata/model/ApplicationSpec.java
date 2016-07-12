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
package org.apache.eagle.metadata.model;

import jdk.nashorn.internal.ir.annotations.Immutable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

/**
 * Static metadata provided by installed apps
 */
@XmlAccessorType(XmlAccessType.FIELD)
@Immutable
public class ApplicationSpec {
    private String type;
    private String name;
    private String version;
    private String description;
    private String classname;
    private String jarpath;
    private String viewpath;

    private ConfigurationSpec configuration;

    public String getDescription() {
        return description;
    }

    public String getVersion() {
        return version;
    }


    public String getType() {
        return type;
    }

    public ConfigurationSpec getConfiguration() {
        return configuration;
    }


    public String getName() {
        return name;
    }

    public String getClassname() {
        return classname;
    }

    public String getViewpath() {
        return viewpath;
    }

    public String getJarpath() {
        return jarpath;
    }
}