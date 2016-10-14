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

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;
import io.dropwizard.auth.Auth;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicAuthProvider;
import io.dropwizard.auth.basic.BasicCredentials;
import org.apache.eagle.server.authentication.authenticator.AbstractSwitchableAuthenticator;

public class SwitchableBasicAuthProvider<P> extends BasicAuthProvider<P> {
    private AbstractSwitchableAuthenticator<BasicCredentials, P> switchableAuthenticator = null;

    public SwitchableBasicAuthProvider(Authenticator<BasicCredentials, P> authenticator, String realm) {
        super(authenticator, realm);
        if (authenticator instanceof AbstractSwitchableAuthenticator) {
            switchableAuthenticator = (AbstractSwitchableAuthenticator<BasicCredentials, P>) authenticator;
        }
    }

    public Injectable<?> getInjectable(ComponentContext ic, Auth a, Parameter c) {
        if (switchableAuthenticator != null && !switchableAuthenticator.getSettings().isEnabled()) {
            return new AbstractHttpContextInjectable<P>() {
                public P getValue(HttpContext c) {
                    return switchableAuthenticator.getUnauthenticatedPrincipal();
                }
            };
        }
        return super.getInjectable(ic, a, c);
    }
}
