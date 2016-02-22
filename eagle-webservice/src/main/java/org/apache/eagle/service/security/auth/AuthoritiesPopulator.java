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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.service.security.auth;


import org.springframework.ldap.core.ContextSource;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.ldap.userdetails.DefaultLdapAuthoritiesPopulator;

import java.util.HashSet;
import java.util.Set;

public class AuthoritiesPopulator extends DefaultLdapAuthoritiesPopulator {

    String adminRole;
    SimpleGrantedAuthority adminRoleAsAuthority;

    SimpleGrantedAuthority adminAuthority = new SimpleGrantedAuthority("ROLE_ADMIN");
    SimpleGrantedAuthority defaultAuthority = new SimpleGrantedAuthority("ROLE_USER");

    /**
     * @param contextSource
     * @param groupSearchBase
     */
    public AuthoritiesPopulator(ContextSource contextSource, String groupSearchBase, String adminRole, String defaultRole) {
        super(contextSource, groupSearchBase);
        this.adminRole = adminRole;
        this.adminRoleAsAuthority = new SimpleGrantedAuthority(adminRole);
    }

    @Override
    public Set<GrantedAuthority> getGroupMembershipRoles(String userDn, String username) {
        Set<GrantedAuthority> authorities = super.getGroupMembershipRoles(userDn, username);
        Set<GrantedAuthority> newAuthorities = new HashSet<>();

        if (authorities.contains(adminRoleAsAuthority)) {
            newAuthorities.add(adminAuthority);
        } else {
            newAuthorities.add(defaultAuthority);
        }

        return newAuthorities;
    }

}