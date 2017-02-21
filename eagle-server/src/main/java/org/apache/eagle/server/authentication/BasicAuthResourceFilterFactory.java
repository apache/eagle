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
package org.apache.eagle.server.authentication;


import com.sun.jersey.api.model.AbstractMethod;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import com.sun.jersey.spi.container.ResourceFilter;
import com.sun.jersey.spi.container.ResourceFilterFactory;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;
import org.apache.eagle.common.security.User;

import java.util.Arrays;
import java.util.List;

public class BasicAuthResourceFilterFactory implements ResourceFilterFactory {
    private final Authenticator<BasicCredentials, User> authenticator;

    public BasicAuthResourceFilterFactory(Authenticator<BasicCredentials, User> authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public List<ResourceFilter> create(AbstractMethod abstractMethod) {
        return Arrays.asList(new ResourceFilter() {
            @Override
            public ContainerRequestFilter getRequestFilter() {
                return new BasicAuthRequestFilter(authenticator, abstractMethod);
            }

            @Override
            public ContainerResponseFilter getResponseFilter() {
                return null;
            }
        });
    }
}
