package org.apache.eagle.app;

import org.apache.eagle.app.config.ApplicationProviderDescConfig;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.InputStream;

/**
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
public class ApplicationProviderDescConfigTest {
    @Test
    public void testApplicationDescWrapperConfigLoadFromXML(){
        ApplicationProviderDescConfig config = ApplicationProviderDescConfig.loadFromXML(null, "TestApplicationMetadata.xml");
        Assert.assertNotNull(config);
    }

    @Test
    public void testStreamDefinitionLoadFromXML(){
        String configXmlFile = "TestStreamDefinitionConf.xml";
        try {
            JAXBContext jc = JAXBContext.newInstance(StreamDefinitions.class);
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            InputStream is = ApplicationProviderDescConfigTest.class.getResourceAsStream(configXmlFile);
            if(is == null){
                is = ApplicationProviderDescConfigTest.class.getResourceAsStream("/"+configXmlFile);
            }
            if(is == null){
                throw new IllegalStateException("Stream Definition configuration "+configXmlFile+" is not found");
            }
            StreamDefinitions streamDefinitions = (StreamDefinitions) unmarshaller.unmarshal(is);
            Assert.assertNotNull(streamDefinitions);
        } catch (Exception ex){
            throw new RuntimeException("Failed to load application descriptor configuration: "+configXmlFile,ex);
        }
    }
}
