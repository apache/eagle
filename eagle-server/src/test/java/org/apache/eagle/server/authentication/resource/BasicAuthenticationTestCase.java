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
package org.apache.eagle.server.authentication.resource;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.UniformInterfaceException;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.apache.eagle.common.Base64;
import org.apache.eagle.common.security.User;
import org.apache.eagle.server.ServerApplication;
import org.apache.eagle.server.ServerConfig;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class BasicAuthenticationTestCase {

    @ClassRule
    public static final DropwizardAppRule<ServerConfig> RULE =
        new DropwizardAppRule<>(ServerApplication.class,
            BasicAuthenticationTestCase.class.getResource("/configuration.yml").getPath());

    private static final String USER_AUTH_KEY = "Basic " + Base64.encode("user:secret");
    private static final String ADMIN_AUTH_KEY = "Basic " + Base64.encode("admin:secret");
    private static final String BAD_AUTH_KEY = "Basic " + Base64.encode("bad:bad");

    @Test
    public void testAuthUserOnly() {
        Client client = new Client();
        client.resource(String.format("http://localhost:%d/rest/testAuth/userOnly", RULE.getLocalPort()))
            .header("Authorization", USER_AUTH_KEY)
            .get(User.class);
    }

    @Test (expected = UniformInterfaceException.class)
    public void testAuthUserOnlyWitBadKey() {
        Client client = new Client();
        client.resource(String.format("http://localhost:%d/rest/testAuth/userOnly", RULE.getLocalPort()))
            .header("Authorization", BAD_AUTH_KEY)
            .get(User.class);
    }

    @Test
    public void testAuthAdminOnly() {
        Client client = new Client();
        client.resource(String.format("http://localhost:%d/rest/testAuth/adminOnly", RULE.getLocalPort()))
            .header("Authorization", ADMIN_AUTH_KEY)
            .get(User.class);
    }

    @Test
    public void testAuthPermitAll() {
        Client client = new Client();
        client.resource(String.format("http://localhost:%d/rest/testAuth/permitAll", RULE.getLocalPort()))
            .header("Authorization", USER_AUTH_KEY)
            .get(User.class);
    }

    @Test
    public void testAuthPermitAllWithoutKeyShouldPass() {
        Client client = new Client();
        try {
            client.resource(String.format("http://localhost:%d/rest/testAuth/permitAll", RULE.getLocalPort()))
                .get(User.class);
        } catch (UniformInterfaceException e) {
            Assert.assertEquals(204, e.getResponse().getStatus());
        }
    }

    @Test
    public void testAuthPermitAllWithBadKeyShouldAccept401() {
        Client client = new Client();
        try {
            client.resource(String.format("http://localhost:%d/rest/testAuth/permitAll", RULE.getLocalPort()))
                .header("Authorization", BAD_AUTH_KEY)
                .get(User.class);
        } catch (UniformInterfaceException e) {
            Assert.assertEquals(401, e.getResponse().getStatus());
        }
    }

    @Test(expected = UniformInterfaceException.class)
    public void testAuthDenyAll() {
        Client client = new Client();
        client.resource(String.format("http://localhost:%d/rest/testAuth/denyAll", RULE.getLocalPort()))
            .header("Authorization", USER_AUTH_KEY)
            .get(User.class);
    }
}
