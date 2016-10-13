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

import io.dropwizard.auth.Authenticator;
import org.apache.eagle.server.authentication.config.AuthenticationSettings;

public abstract class AbstractSwitchableAuthenticator<C, P> implements Authenticator<C, P> {
    private AuthenticationSettings settings;
    private Class<P> principalClass;
    private P unauthenticatedPrincipal;

    public AbstractSwitchableAuthenticator(AuthenticationSettings settings, Class<P> principalClass) {
        this.settings = settings;
        this.principalClass = principalClass;
    }

    public AuthenticationSettings getSettings() {
        return settings;
    }

    public P getUnauthenticatedPrincipal() {
        try {
            if (unauthenticatedPrincipal == null) {
                unauthenticatedPrincipal = principalClass.newInstance();
            }
            return unauthenticatedPrincipal;
        } catch (InstantiationException e) {
            throw new RuntimeException(String.format("Filed to instantiate %s", principalClass.getName()), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(String.format("Illegal access to %s", principalClass.getName()), e);
        }
    }
}
