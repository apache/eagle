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
package org.apache.eagle.common.authentication;

import java.io.Serializable;
import java.security.Principal;
import java.util.Set;

public class User implements Principal, Serializable {
    private String username = "Unauthenticated";
    private Set<String> roles = null;

    public User() {
    }

    public User(String username) {
        this.username = username;
    }

    public User(String username, Set<String> roles) {
        this.username = username;
        this.roles = roles;
    }

    public Set<String> getRoles() {
        return roles;
    }

    public String getName() {
        return username;
    }
}
