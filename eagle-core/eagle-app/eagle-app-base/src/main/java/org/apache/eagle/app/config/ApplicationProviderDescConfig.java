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


import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.metadata.model.ApplicationDocs;
import org.apache.eagle.metadata.model.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.*;

@XmlRootElement(name = "application")
@XmlAccessorType(XmlAccessType.FIELD)
public class ApplicationProviderDescConfig {
    private String type;
    private String name;
    private String version;
    private String description;
    private String appClass;
    private String viewPath;
    private Configuration configuration;
    private ApplicationDocs docs;

    @XmlElementWrapper(name = "streams")
    @XmlElement(name = "stream")
    private List<StreamDefinition> streams;

    public String getDescription() {
        return description;
    }

    public String getVersion() {
        return version;
    }

    public String getType() {
        return type;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public String getName() {
        return name;
    }

    public String getViewPath() {
        return viewPath;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setViewPath(String viewPath) {
        this.viewPath = viewPath;
    }

    @Override
    public String toString() {
        return String.format("ApplicationDesc [type=%s, name=%s, version=%s, appClass=%s, viewPath=%s, configuration= %s properties, description=%s",
            getType(), getName(), getVersion(), getAppClass(), getViewPath(), getConfiguration() == null ? 0 : getConfiguration().size(), getDescription());
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }


    public List<StreamDefinition> getStreams() {
        return streams;
    }

    public void setStreams(List<StreamDefinition> streams) {
        this.streams = streams;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationProviderDescConfig.class);

    public static ApplicationProviderDescConfig loadFromXML(Class<?> classLoader, String configXmlFile) {
        try {
            JAXBContext jc = JAXBContext.newInstance(ApplicationProviderDescConfig.class);
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            // InputStream is = ApplicationProviderDescConfig.class.getResourceAsStream(configXmlFile);
            InputStream is = classLoader.getResourceAsStream(configXmlFile);
            if (is == null) {
                is = ApplicationProviderDescConfig.class.getResourceAsStream("/" + configXmlFile);
            }
            if (is == null) {
                LOG.error("Application descriptor configuration {} is not found", configXmlFile);
                throw new IllegalStateException("Application descriptor configuration " + configXmlFile + " is not found");
            }
            return (ApplicationProviderDescConfig) unmarshaller.unmarshal(is);
        } catch (Exception ex) {
            LOG.error("Failed to load application descriptor configuration: {}", configXmlFile, ex);
            throw new RuntimeException("Failed to load application descriptor configuration: " + configXmlFile, ex);
        }
    }

    public String getAppClass() {
        return appClass;
    }

    public void setAppClass(String appClass) {
        this.appClass = appClass;
    }

    public ApplicationDocs getDocs() {
        return docs;
    }

    public void setDocs(ApplicationDocs docs) {
        this.docs = docs;
    }
}
