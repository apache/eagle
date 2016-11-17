/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.server.authentication.authenticator;

import com.google.common.base.Optional;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.basic.BasicCredentials;
import org.apache.eagle.common.authentication.User;
import org.apache.eagle.server.authentication.config.SimpleSettings;
import org.junit.Assert;
import org.junit.Test;

public class SimpleBasicAuthenticatorTest {

    private static final String TEST_USERNAME = "normal-username";
    private static final String TEST_SECRET_PHRASE = "secret-phrase";
    private static final String TEST_UNEXISTING_USERNAME = "unexisting-username";
    private static final String TEST_WRONG_SECRET_PHRASE = "wrong-secret-phrase";
    private static SimpleBasicAuthenticator authenticator = new SimpleBasicAuthenticator(new SimpleSettings().setUsername(TEST_USERNAME).setPassword(TEST_SECRET_PHRASE));

    @Test
    public void testNormal() {
        try {
            BasicCredentials credentials = new BasicCredentials(TEST_USERNAME, TEST_SECRET_PHRASE);
            Optional<User> result = authenticator.authenticate(credentials);
            Assert.assertTrue("result isn't present when passed correct credentials", result.isPresent());
            User user = result.get();
            Assert.assertEquals("authenticated user is not expected", TEST_USERNAME, user.getName());
        }
        catch (AuthenticationException e) {
            Assert.fail("unexpected error occurs: "+e.getMessage());
        }
    }

    @Test
    public void testUnexistingUsername() {
        try {
            Optional<User> result = authenticator.authenticate(new BasicCredentials(TEST_UNEXISTING_USERNAME, TEST_SECRET_PHRASE));
            Assert.assertFalse("result is present when passed unexisting username", result.isPresent());
        }
        catch (AuthenticationException e) {
            Assert.fail("unexpected error occurs: "+e.getMessage());
        }
    }

    @Test
    public void testWrongPassword() {
        try {
            Optional<User> result = authenticator.authenticate(new BasicCredentials(TEST_USERNAME, TEST_WRONG_SECRET_PHRASE));
            Assert.assertFalse("result is present when passed wrong password", result.isPresent());
        }
        catch (AuthenticationException e) {
            Assert.fail("unexpected error occurs: "+e.getMessage());
        }
    }
}
