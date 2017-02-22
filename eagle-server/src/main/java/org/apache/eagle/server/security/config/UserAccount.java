/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.server.security.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.eagle.common.security.User;

import java.util.*;

public class UserAccount extends User {

    @JsonProperty("password")
    public String getEncryptedPassword() {
        return encryptedPassword;
    }

    private String encryptedPassword;

    public UserAccount() {

    }

    public UserAccount(String username, String encryptedPassword) {
        this.setName(username);
        this.encryptedPassword = encryptedPassword;
    }

    public void setRoles(String roles) {
        if (roles != null) {
            String[] roleStrArray = roles.split(",");
            Set<User.Role> rolesSet = new HashSet<>();
            for (String roleStr:roleStrArray) {
                User.Role role = User.Role.locateCaseInsensitive(roleStr.trim());
                Preconditions.checkNotNull(role, "Invalid role " + roleStr);
                rolesSet.add(role);
            }
            super.setRoles(rolesSet);
        }
    }
}
