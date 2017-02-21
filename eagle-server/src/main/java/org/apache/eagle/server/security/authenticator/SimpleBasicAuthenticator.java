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
package org.apache.eagle.server.security.authenticator;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;
import org.apache.eagle.common.security.User;
import org.apache.eagle.server.security.config.SimpleConfig;
import org.apache.eagle.server.security.config.UserAccount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SimpleBasicAuthenticator implements Authenticator<BasicCredentials, User> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleBasicAuthenticator.class);
    private final Map<String, UserAccount> userAccountRepository;

    public SimpleBasicAuthenticator(SimpleConfig config) {
        userAccountRepository = new HashMap<>();
        for (UserAccount userAccount : config.getAccounts()) {
            Preconditions.checkNotNull(userAccount.getName(), " Username is null " + userAccount);
            Preconditions.checkArgument(!userAccountRepository.containsKey(userAccount.getName()), "Duplicated user name: " + userAccount.getName());
            if (userAccount.getRoles() == null) {
                LOGGER.warn("UserPrincipal {} has no roles, set as {} by default", userAccount.getName(), User.Role.USER);
                userAccount.setRoles(User.Role.USER.toString());
            }
            userAccountRepository.put(userAccount.getName(), userAccount);
        }
    }

    public Optional<User> authenticate(BasicCredentials credentials) throws AuthenticationException {
        if (userAccountRepository.containsKey(credentials.getUsername())
            && Objects.equals(userAccountRepository.get(credentials.getUsername()).getPassword(), credentials.getPassword())) {
            UserAccount userAccount = userAccountRepository.get(credentials.getUsername());
            return Optional.of(new User(userAccount));
        } else {
            return Optional.absent();
        }
    }
}