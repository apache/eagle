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
package org.apache.eagle.common.security;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.security.Principal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@JsonSerialize
public class User implements Principal, Serializable {
    private String username;
    private String firstName;
    private String lastName;
    private String email;
    private String fullName;

    private Collection<Role> roles;

    public User() {

    }

    public User(User user) {
        this.setName(user.getName());
        this.setFirstName(user.getFirstName());
        this.setLastName(user.getLastName());
        this.setEmail(user.getEmail());
        this.setRoles(user.getRoles());
    }

    public User(String username) {
        this.username = username;
    }

    public User(String username, Collection<Role> roles) {
        this.username = username;
        this.roles = roles;
    }

    public Collection<Role> getRoles() {
        return roles;
    }

    public void setRoles(Collection<Role> roles) {
        this.roles = roles;
    }

    public String getFullName() {
        if (this.fullName !=null ) {
            return this.fullName;
        }
        if (this.firstName == null && this.lastName == null) {
            return this.username;
        } else if (this.firstName != null && this.lastName == null ) {
            return this.firstName;
        } else if (this.firstName == null) {
            return this.lastName;
        } else {
            return String.format("%s, %s", this.lastName, this.firstName);
        }
    }

    @Override
    public String getName() {
        return this.username;
    }

    public void setName(String username) {
        this.username = username;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    @Override
    public String toString() {
        return "User {"
            + "name='" + username + '\''
            + ", firstName='" + firstName + '\''
            + ", lastName='" + lastName + '\''
            + ", email='" + email + '\''
            + ", fullName='" + fullName + '\''
            + ", roles=" + roles
            + '}';
    }

    public enum Role implements Serializable {
        USER("USER"),                     // USER role with user-level permissions
        APPLICATION("APPLICATION"),       // APPLICATION role with application-level permissions
        ADMINISTRATOR("ADMINISTRATOR");   // ADMINISTRATOR role with admin-level permissions

        public static final Role[] ALL_ROLES = new Role[] {
            USER,APPLICATION, ADMINISTRATOR
        };

        private static Map<String,Role> nameRoleMap = new HashMap<String,Role>() {
            {
                put(ADMINISTRATOR.roleName.toUpperCase(), ADMINISTRATOR);
                put(APPLICATION.roleName.toUpperCase(), APPLICATION);
                put(USER.roleName.toUpperCase(), USER);
            }
        };

        Role(String roleName) {
            this.roleName = roleName;
        }

        @Override
        public String toString() {
            return roleName;
        }

        public static Role locateCaseInsensitive(String roleName) {
            Preconditions.checkArgument(nameRoleMap.containsKey(roleName.toUpperCase()), "Illegal role " + roleName);
            return nameRoleMap.get(roleName.toUpperCase());
        }

        private final String roleName;
    }

    public boolean isInRole(Role ... allowedRoles) {
        Preconditions.checkNotNull(allowedRoles);
        if (this.roles != null ) {
            for (Role allowRole: allowedRoles) {
                if (this.roles.contains(allowRole)) {
                    return true;
                }
            }
        }
        return false;
    }
}