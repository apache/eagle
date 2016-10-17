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

import io.dropwizard.setup.Environment;
import org.apache.eagle.common.authentication.User;
import org.apache.eagle.server.ServerConfig;

import java.security.Principal;

public class AuthenticationRegister<P extends Principal> {
    private ServerConfig serverConfig = null;
    private Environment environment = null;
    private Class<P> principalClass = null;

    public AuthenticationRegister(ServerConfig serverConfig, Environment environment, Class<P> principalClass) {
        this.serverConfig = serverConfig;
        this.environment = environment;
        this.principalClass = principalClass;
    }

    public void register() {
        AuthenticationMode<User> mode = AuthenticationModeIdentifier.initiate(serverConfig.getAuth(), environment).identify();

        environment.jersey().register(new SwitchableBasicAuthProvider(mode.getAuthenticator(), mode.getRealm()));
    }
}
