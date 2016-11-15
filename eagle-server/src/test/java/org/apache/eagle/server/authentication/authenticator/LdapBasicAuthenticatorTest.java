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

import io.dropwizard.util.Duration;
import org.apache.eagle.server.authentication.config.LdapSettings;
import org.junit.Assert;
import org.junit.Test;

import javax.naming.Context;
import java.util.Hashtable;

public class LdapBasicAuthenticatorTest {

    private static final String USERNAME_SUFFIX = "@some.emailbox.suffix";
    private static final String USERNAME_TEMPLATE = "${USERNAME}" + USERNAME_SUFFIX;
    private static final String LDAP_SERVICE_PROVIDER_URL = "ldap://some.address:port";
    private static final String STRATEGY_SIMPLE = "customized";
    private static final String CONNECTING_TIMEOUT_VALUE = "500ms";
    private static final String READING_TIMEOUT_VALUE = "800ms";
    private static final String LDAP_CTX_FACTORY_NAME = "com.sun.jndi.ldap.LdapCtxFactory";
    private static final String LDAP_CONNECT_TIMEOUT_KEY = "com.sun.jndi.ldap.connect.timeout";
    private static final String LDAP_READ_TIMEOUT_KEY = "com.sun.jndi.ldap.read.timeout";
    private static final LdapBasicAuthenticator AUTHENTICATOR_FOR_UTIL_METHODS = new LdapBasicAuthenticator(
            new LdapSettings()
                    .setProviderUrl(LDAP_SERVICE_PROVIDER_URL)
                    .setPrincipalTemplate(USERNAME_TEMPLATE)
                    .setStrategy(STRATEGY_SIMPLE)
                    .setConnectingTimeout(Duration.parse(CONNECTING_TIMEOUT_VALUE))
                    .setReadingTimeout(Duration.parse(READING_TIMEOUT_VALUE))
    );

    @Test
    public void testSanitizeUsername() {
        String correctUsername = "userNAME_123.45Z";
        String sanitized = AUTHENTICATOR_FOR_UTIL_METHODS.sanitizeUsername(correctUsername);
        Assert.assertEquals(correctUsername, sanitized);

        String incorrectUsername = "userNAME-~!@#$%^&777*()_+-=`[]\\{}|;':\",./<>?你";
        sanitized = AUTHENTICATOR_FOR_UTIL_METHODS.sanitizeUsername(incorrectUsername);
        System.out.println(sanitized);
        Assert.assertEquals("userNAME777_.", sanitized);
    }

    @Test
    public void testComprisePrincipal() {
        String username = "my.userNAME_123";
        String principal = AUTHENTICATOR_FOR_UTIL_METHODS.comprisePrincipal(username);
        Assert.assertEquals(username+USERNAME_SUFFIX, principal);
    }

    @Test
    public void testGetContextEnvironment() {
        String username = "username";
        String secret_phrase = "secret-phrase";
        Hashtable<String, String> env = AUTHENTICATOR_FOR_UTIL_METHODS.getContextEnvironment(username, secret_phrase);

        Assert.assertEquals("unexpected ldap context factory name", LDAP_CTX_FACTORY_NAME, env.get(Context.INITIAL_CONTEXT_FACTORY));
        Assert.assertEquals("unexpected ldap serivce provider url", LDAP_SERVICE_PROVIDER_URL, env.get(Context.PROVIDER_URL));
        Assert.assertEquals("unexpected connecting timeout value", String.valueOf(Duration.parse(CONNECTING_TIMEOUT_VALUE).toMilliseconds()), env.get(LDAP_CONNECT_TIMEOUT_KEY));
        Assert.assertEquals("unexpected reading timeout value", String.valueOf(Duration.parse(READING_TIMEOUT_VALUE).toMilliseconds()), env.get(LDAP_READ_TIMEOUT_KEY));
        Assert.assertEquals("unexpected username", username+USERNAME_SUFFIX, env.get(Context.SECURITY_PRINCIPAL));
        Assert.assertEquals("unexpected secret credentials", secret_phrase, env.get(Context.SECURITY_CREDENTIALS));
        Assert.assertEquals("unexpected strategy", STRATEGY_SIMPLE, env.get(Context.SECURITY_AUTHENTICATION));

        // check strategy while it's configured as ""
        LdapBasicAuthenticator blankStrategyAuthenticator = new LdapBasicAuthenticator(new LdapSettings().setStrategy(""));
        String strategyMaybeBlank = blankStrategyAuthenticator.getContextEnvironment(username, secret_phrase).get(Context.SECURITY_AUTHENTICATION);
        Assert.assertNull("unexpected strategy", strategyMaybeBlank);
    }
}
