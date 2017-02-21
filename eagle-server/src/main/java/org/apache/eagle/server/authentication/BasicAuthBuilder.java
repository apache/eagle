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
package org.apache.eagle.server.authentication;

import com.google.common.cache.CacheBuilderSpec;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;
import io.dropwizard.auth.Auth;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.CachingAuthenticator;
import io.dropwizard.auth.basic.BasicAuthProvider;
import io.dropwizard.auth.basic.BasicCredentials;
import io.dropwizard.setup.Environment;
import org.apache.eagle.common.security.User;
import org.apache.eagle.server.authentication.authenticator.LdapBasicAuthenticator;
import org.apache.eagle.server.authentication.authenticator.SimpleBasicAuthenticator;
import org.apache.eagle.server.authentication.config.AuthenticationConfig;

import java.util.Arrays;

public class BasicAuthBuilder {
    private static final String SIMPLE_MODE_REALM = "SIMPLE_BASIC_AUTHENTICATION";
    private static final String LDAP_MODE_REALM = "LDAP_BASIC_AUTHENTICATION";
    private final Authenticator<BasicCredentials, User> authenticator;
    private final BasicAuthProvider basicAuthProvider;
    private AuthenticationConfig authConfig;
    private Environment environment;

    public BasicAuthBuilder(AuthenticationConfig authConfig, Environment environment) {
        this.authConfig = authConfig;
        this.environment = environment;
        boolean needsCaching = authConfig.needsCaching();
        Authenticator<BasicCredentials, User> authenticator;
        String realm;
        if (authConfig.isEnabled()) {
            switch (authConfig.getMode()) {
                case "simple":
                    authenticator = new SimpleBasicAuthenticator(authConfig.getSimple());
                    realm = SIMPLE_MODE_REALM;
                    break;
                case "ldap":
                    authenticator = new LdapBasicAuthenticator(authConfig.getLdap());
                    realm = LDAP_MODE_REALM;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid auth mode " + authConfig.getMode());
            }
            if (needsCaching) {
                authenticator = cache(authenticator);
            }
            this.authenticator = authenticator;
            this.basicAuthProvider = new BasicAuthProvider<>(this.authenticator, realm);
        } else {
            this.authenticator = null;
            this.basicAuthProvider = new BasicAuthProvider<User>(null, "") {
                public Injectable<User> getInjectable(ComponentContext ic, Auth a, Parameter c) {
                    return new AbstractHttpContextInjectable<User>() {
                        public User getValue(HttpContext c) {
                            User user =  new User();
                            user.setName("anonymous");
                            user.setFirstName("Anonymous User (auth: false)");
                            user.setRoles(Arrays.asList(User.Role.ALL_ROLES));
                            return user;
                        }
                    };
                }
            };
        }
    }

    public BasicAuthProvider getBasicAuthProvider() {
        return this.basicAuthProvider;
    }

    public Authenticator<BasicCredentials, User> getBasicAuthenticator() {
        return this.authenticator;
    }

    private Authenticator<BasicCredentials, User> cache(Authenticator<BasicCredentials, User> authenticator) {
        return new CachingAuthenticator<>(environment.metrics(), authenticator, CacheBuilderSpec.parse(authConfig.getCachePolicy()));
    }
}
