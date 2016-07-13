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
package org.apache.eagle.metadata;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.eagle.metadata.model.ApplicationsConfig;
import org.apache.eagle.metadata.service.ApplicationSpecService;
import org.apache.eagle.metadata.store.MetadataStore;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.InputStream;

public class ApplicationSpecServiceTest {
    @Test
    public void testApplicationSpecServiceInjection(){
        Injector injector = Guice.createInjector(MetadataStore.getInstance());
        ApplicationSpecService service_1 = injector.getInstance(ApplicationSpecService.class);
        ApplicationSpecService service_2 = injector.getInstance(ApplicationSpecService.class);
        Assert.assertNotNull(service_1);
        Assert.assertNotNull(service_2);
        Assert.assertEquals(service_1,service_2);
    }

    @Test
    public void testBasicUnmarshal() throws JAXBException, IOException {
        JAXBContext jc = JAXBContext.newInstance(ApplicationsConfig.class);
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        InputStream is = ApplicationSpecServiceTest.class.getResourceAsStream("/applications.xml");
        Assert.assertNotNull("/applications.xml should not be null",is);
        ApplicationsConfig applicationsConfig = (ApplicationsConfig) unmarshaller.unmarshal(is);
        Assert.assertEquals(1, applicationsConfig.getApplications().size());
    }
}