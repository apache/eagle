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
package org.apache.eagle.metadata.utils;

import com.google.common.base.Preconditions;
import org.apache.eagle.metadata.model.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.InputStream;
import java.io.StringReader;

public class ConfigTemplateHelper {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigTemplateHelper.class);

    public static Configuration unmarshallFromXmlStream(InputStream inputStream) throws JAXBException {
        Preconditions.checkNotNull(inputStream, "Input stream is null");
        try {
            JAXBContext jc = JAXBContext.newInstance(Configuration.class);
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            return (Configuration) unmarshaller.unmarshal(inputStream);
        } catch (Exception ex) {
            LOG.error("Failed to unmarshall ConfigTemplate from stream", ex);
            throw ex;
        }
    }

    public static Configuration unmarshallFromResource(String resourceName) throws JAXBException {
        String source = resourceName;
        InputStream inputStream = ConfigTemplateHelper.class.getResourceAsStream(resourceName);
        if (inputStream == null) {
            source = "/" + resourceName;
            // LOG.debug("Unable to get resource from {}, retrying with ",resourceName,source);
            inputStream = ConfigTemplateHelper.class.getResourceAsStream(source);
        }
        Preconditions.checkNotNull(inputStream, "Unable to load stream from resource " + source);
        Configuration configuration = unmarshallFromXmlStream(inputStream);
        return configuration;
    }

    public static Configuration unmarshallFromXMLString(String xmlConfiguration) throws JAXBException {
        try {
            JAXBContext jc = JAXBContext.newInstance(Configuration.class);
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            return (Configuration) unmarshaller.unmarshal(new StringReader(xmlConfiguration));
        } catch (Exception ex) {
            LOG.error("Failed to unmarshall ConfigTemplate from string", ex);
            throw ex;
        }
    }
}