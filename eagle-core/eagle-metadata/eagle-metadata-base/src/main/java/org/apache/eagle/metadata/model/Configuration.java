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

import org.apache.eagle.metadata.utils.ConfigTemplateHelper;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.InputStream;
import java.util.List;

@XmlRootElement(name = "configuration")
@XmlAccessorType(XmlAccessType.FIELD)
public class Configuration {
    @XmlElement(name = "property")
    private List<Property> properties;

    public List<Property> getProperties() {
        return properties;
    }

    public Property getProperty(String name){
        for(Property property :properties){
            if(property.getName().equals(name)){
                return property;
            }
        }
        return null;
    }

    public boolean hasProperty(String name){
        for(Property property :properties){
            if(property.getName().equals(name)){
                return true;
            }
        }
        return false;
    }

    public static Configuration fromStream(InputStream inputStream) throws JAXBException {
        return ConfigTemplateHelper.unmarshallFromXmlStream(inputStream);
    }

    public static Configuration fromResource(String resourceName) throws JAXBException {
        return ConfigTemplateHelper.unmarshallFromResource(resourceName);
    }
    public static Configuration fromString(String xmlContent) throws JAXBException {
        return ConfigTemplateHelper.unmarshallFromXMLString(xmlContent);
    }

    public int size(){
        if(this.properties == null){
            return 0;
        }
        return this.properties.size();
    }
}