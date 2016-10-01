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
import com.typesafe.config.Config;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;
import org.apache.eagle.server.authentication.principal.User;

public class LdapAuthenticator implements Authenticator<BasicCredentials, User> {
    public static final String URI_KEY = "auth.ldap.uri";
    public static final String USER_FILTER_KEY = "auth.ldap.userFilter";
    public static final String GROUP_FILTER_KEY = "auth.ldap.groupFilter";
    public static final String USER_NAME_ATTRIBUTE_KEY = "auth.ldap.userNameAttribute";
    public static final String GROUP_NAME_ATTRIBUTE_KEY = "auth.ldap.groupNameAttribute";
    public static final String GROUP_MEMBERSHIP_ATTRIBUTE_KEY = "auth.ldap.groupMembershipAttribute";
    public static final String GROUP_CLASS_NAME_KEY = "auth.ldap.groupClassName";
    public static final String RESTRICT_TO_GROUPS_KEY = "auth.ldap.restrictToGroups";
    public static final String CONNECT_TIMEOUT_KEY = "auth.ldap.connectTimeout";
    public static final String READ_TIMEOUT_KEY = "auth.ldap.readTimeout";
    private Config config = null;
    public LdapAuthenticator(Config config) {
        this.config = config;
    }

    @Override
    public Optional<User> authenticate(BasicCredentials credentials) throws AuthenticationException {
        // TODO need to implement ldap authentication logic
        boolean pass = true;
        if (pass)
            return Optional.of(new User("ldap.username"));
        return Optional.absent();
    }
}
