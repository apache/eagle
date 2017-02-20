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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UserPrincipal implements Principal, Serializable {
    private String username;
    private List<Role> roles;

    public UserPrincipal() {

    }

    public UserPrincipal(String username) {
        this.username = username;
    }

    public UserPrincipal(String username, List<Role> roles) {
        this.username = username;
        this.roles = roles;
    }

    public List<Role> getRoles() {
        return roles;
    }

    public String getName() {
        return username;
    }

    public enum Role {
        ADMIN_ROLE("ADMIN"),
        USER_ROLE("USER");

        private static Map<String,Role> nameRoleMap = new HashMap<String,Role>() {
            {
                put(ADMIN_ROLE.roleName, ADMIN_ROLE);
                put(USER_ROLE.roleName, USER_ROLE);
            }
        };

        Role(String roleName) {
            this.roleName = roleName;
        }

        @Override
        public String toString() {
            return roleName;
        }

        public static Role locate(String roleName) {
            return nameRoleMap.get(roleName);
        }

        private final String roleName;
    }
}