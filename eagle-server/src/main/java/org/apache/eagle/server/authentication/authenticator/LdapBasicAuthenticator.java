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
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;
import org.apache.eagle.common.authentication.User;
import org.apache.eagle.server.authentication.config.LdapSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.directory.InitialDirContext;
import java.util.Hashtable;

public class LdapBasicAuthenticator implements Authenticator<BasicCredentials, User> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LdapBasicAuthenticator.class);
    private static final String LDAP_LDAP_CTX_FACTORY_NAME = "com.sun.jndi.ldap.LdapCtxFactory";
    private static final String LDAP_CONNECT_TIMEOUT_KEY = "com.sun.jndi.ldap.connect.timeout";
    private static final String LDAP_READ_TIMEOUT_KEY = "com.sun.jndi.ldap.read.timeout";
    private LdapSettings settings = null;

    public LdapBasicAuthenticator(LdapSettings settings) {
        this.settings = settings;
    }

    public Optional<User> authenticate(BasicCredentials credentials) throws AuthenticationException {
        String sanitizedUsername = sanitizeUsername(credentials.getUsername());
        try {
            new InitialDirContext(getContextEnvironment(sanitizedUsername, credentials.getPassword()));
            return Optional.of(new User(sanitizedUsername));
        } catch (javax.naming.AuthenticationException ae) {
            LOGGER.warn(String.format("Authentication failed for user[%s]: wrong username or password", sanitizedUsername));
            return Optional.absent();
        } catch (Exception e) {
            throw new AuthenticationException(String.format("Error occurs while trying to authenticate for user[%s]: %s", sanitizedUsername, e.getMessage()), e);
        }
    }

    Hashtable<String, String> getContextEnvironment(String sanitizedUsername, String password) {
        String providerUrl = settings.getProviderUrl();
        if (providerUrl == null) {
            throw new IllegalArgumentException("providerUrl of the ldap service shouldn't be null");
        }

        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, LDAP_LDAP_CTX_FACTORY_NAME);
        env.put(Context.PROVIDER_URL, providerUrl);
        env.put(LDAP_CONNECT_TIMEOUT_KEY, String.valueOf(settings.getConnectingTimeout().toMilliseconds()));
        env.put(LDAP_READ_TIMEOUT_KEY, String.valueOf(settings.getReadingTimeout().toMilliseconds()));

        String strategy = settings.getStrategy();
        if (!"".equals(strategy)) {
            env.put(Context.SECURITY_AUTHENTICATION, strategy);
        }

        env.put(Context.SECURITY_PRINCIPAL, comprisePrincipal(sanitizedUsername));
        env.put(Context.SECURITY_CREDENTIALS, password);
        return env;
    }

    String comprisePrincipal(String sanitizedUsername) {
        return settings.getPrincipalTemplate().replaceAll("\\$\\{USERNAME\\}", sanitizedUsername);
    }

    String sanitizeUsername(String username) {
        return username.replaceAll("[^a-zA-Z0-9_.]", "");
    }

}
