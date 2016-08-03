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
package org.apache.eagle.app.spi;

import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.config.ApplicationProviderConfig;
import org.apache.eagle.app.config.ApplicationProviderDescConfig;
import org.apache.eagle.app.sink.KafkaStreamSink;
import org.apache.eagle.app.sink.StreamSink;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationDocs;
import org.apache.eagle.metadata.model.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.util.List;

public abstract class AbstractApplicationProvider<T extends Application> implements ApplicationProvider<T> {
    private final static Logger LOG = LoggerFactory.getLogger(AbstractApplicationProvider.class);
    private final ApplicationDesc applicationDesc;

    public AbstractApplicationProvider(){
        applicationDesc = new ApplicationDesc();
        applicationDesc.setProviderClass(this.getClass());
        configure();
    }

    protected void configure (){
        // do nothing by default
    }

    protected AbstractApplicationProvider(String applicationDescConfig) {
        this();
        ApplicationProviderDescConfig descWrapperConfig = ApplicationProviderDescConfig.loadFromXML(applicationDescConfig);
        setType(descWrapperConfig.getType());
        setVersion(descWrapperConfig.getVersion());
        setName(descWrapperConfig.getName());
        setDocs(descWrapperConfig.getDocs());
        try {
            if (descWrapperConfig.getAppClass() != null) {
                setAppClass((Class<T>) Class.forName(descWrapperConfig.getAppClass()));
                if (!Application.class.isAssignableFrom(applicationDesc.getAppClass())) {
                    throw new IllegalStateException(descWrapperConfig.getAppClass() + " is not sub-class of " + Application.class.getCanonicalName());
                }
            }
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
        setViewPath(descWrapperConfig.getViewPath());
        setConfiguration(descWrapperConfig.getConfiguration());
        setStreams(descWrapperConfig.getStreams());
    }

    @Override
    public void prepare(ApplicationProviderConfig providerConfig, Config envConfig) {
        this.applicationDesc.setJarPath(providerConfig.getJarPath());
//        String sinkClassName = envConfig.hasPath(APPLICATIONS_SINK_TYPE_PROPS_KEY) ?
//                envConfig.getString(APPLICATIONS_SINK_TYPE_PROPS_KEY) : DEFAULT_APPLICATIONS_SINK_TYPE;
//        try {
//            Class<?> sinkClass = Class.forName(sinkClassName);
//            if(!StreamSink.class.isAssignableFrom(sinkClass)){
//                throw new IllegalStateException(sinkClassName+ "is not assignable from "+StreamSink.class.getCanonicalName());
//            }
//            applicationDesc.setSinkClass(sinkClass);
//        } catch (ClassNotFoundException e) {
//            throw new IllegalStateException(e.getMessage(),e.getCause());
//        }
    }

    protected void setVersion(String version) {
        applicationDesc.setVersion(version);
    }

    protected void setName(String name) {
        applicationDesc.setName(name);
    }

    protected void setAppClass(Class<T> appClass) {
        applicationDesc.setAppClass(appClass);
    }

    protected void setViewPath(String viewPath) {
        applicationDesc.setViewPath(viewPath);
    }

    protected void setConfiguration(Configuration configuration) {
        applicationDesc.setConfiguration(configuration);
    }

    protected void setAppConfig(String resourceName) {
        try {
            applicationDesc.setConfiguration(Configuration.fromResource(resourceName));
        } catch (JAXBException e) {
            LOG.error("Failed to load configuration template from "+resourceName,e);
            throw new RuntimeException("Failed to load configuration template from "+resourceName,e);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "%s[name=%s, type=%s, version=%s, viewPath=%s, appClass=%s, configuration= %s properties]", getClass().getSimpleName(),
                applicationDesc.getName(),applicationDesc.getType(),applicationDesc.getVersion(), applicationDesc.getViewPath(), applicationDesc.getAppClass(), applicationDesc.getConfiguration().size()
        );
    }

    protected void setStreams(List<StreamDefinition> streams) {
        applicationDesc.setStreams(streams);
    }


    protected void setDocs(ApplicationDocs docs) {
        applicationDesc.setDocs(docs);
    }

    public void setType(String type) {
        applicationDesc.setType(type);
    }

    @Override
    public ApplicationDesc getApplicationDesc() {
        return applicationDesc;
    }
}