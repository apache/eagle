/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.common.service;


import org.apache.eagle.common.config.EagleConfig;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import java.util.*;

/**
 * @since : 7/11/14,2014
 */
public class LdapService {
    private final static Logger LOG = LoggerFactory.getLogger(LdapService.class);

    private final List<String> ldapSrvs;
    private String ldapCerts;
    private final String securityPrincipal;
    private final String securityCredentials;

    public final static String SECURITY_PRINCIPAL_CONFIG_NAME = "eagle.ldap.security-principal";
    public final static String SECURITY_CREDENTIALS_CONFIG_NAME = "eagle.ldap.security-credentials";
    public final static String LDAP_SERVER_CONFIG_NAME = "eagle.ldap.server";
    public final static String LDAP_CERTS_CONFIG_NAME = "eagle.ldap.certs";
    public final static String DEFAULT_LDAP_CERTS_FILE_NAME = "jssecacerts";

    private LdapService(){
        EagleConfig manager = EagleConfigFactory.load();
        securityPrincipal = manager.getConfig().getString(SECURITY_PRINCIPAL_CONFIG_NAME);
        securityCredentials = manager.getConfig().getString(SECURITY_CREDENTIALS_CONFIG_NAME);
        String ldapServer = manager.getConfig().getString(LDAP_SERVER_CONFIG_NAME);
        if(LOG.isDebugEnabled())
            LOG.debug(SECURITY_PRINCIPAL_CONFIG_NAME+":"+securityPrincipal);
        if(securityCredentials!=null){
            if(LOG.isDebugEnabled())
                LOG.debug(SECURITY_CREDENTIALS_CONFIG_NAME+": (hidden for security, length: "+securityCredentials.length()+")");
        }else{
            LOG.warn(SECURITY_CREDENTIALS_CONFIG_NAME+":"+null);
        }
        if(LOG.isDebugEnabled())
            LOG.debug(LDAP_SERVER_CONFIG_NAME+":"+ldapServer);

        ldapSrvs = Arrays.asList(ldapServer.split(","));
        ldapCerts = manager.getConfig().getString(LDAP_CERTS_CONFIG_NAME);
        if(ldapCerts == null) {
            ldapCerts = LdapService.class.getClassLoader().getResource(DEFAULT_LDAP_CERTS_FILE_NAME).getPath();
        }else if(!ldapCerts.startsWith("/") && !ldapCerts.matches("[a-zA-Z]+:.*")) {
            ldapCerts = LdapService.class.getClassLoader().getResource(ldapCerts).getPath();
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug(SECURITY_PRINCIPAL_CONFIG_NAME +": "+securityPrincipal);
            if(securityCredentials == null){
                LOG.debug(SECURITY_CREDENTIALS_CONFIG_NAME +": null");
            }else{
                LOG.debug(SECURITY_CREDENTIALS_CONFIG_NAME +": (hidden, length: "+securityCredentials .length()+")");
            }

            LOG.debug(LDAP_SERVER_CONFIG_NAME +": "+ldapSrvs);
            LOG.debug(LDAP_CERTS_CONFIG_NAME +": "+ldapCerts);
        }
    }

    private static LdapService instance;

    public static LdapService getInstance(){
        if(instance == null){
            instance = new LdapService();
        }
        return instance;
    }

    protected DirContext getDirContext(int id) {
        if (ldapCerts != null) {
            System.setProperty("javax.net.ssl.keyStore", ldapCerts);
            System.setProperty("javax.net.ssl.trustStore", ldapCerts);
        }

        String host = ldapSrvs.get(id);

        Hashtable<String, String> env = new Hashtable<String, String>();
//		if (ldapCerts != null) {
        env.put(Context.SECURITY_PROTOCOL, "ssl");
//		}
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, host);
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, securityPrincipal);
        env.put(Context.SECURITY_CREDENTIALS, securityCredentials);
        env.put("java.naming.ldap.attributes.binary", "objectSID");
        env.put("java.naming.ldap.factory.socket","hadoop.eagle.common.service.TrustAllSSLSocketFactory");

        DirContext ctx = null;
        try {
            ctx = new InitialDirContext(env);
        } catch (Exception e) {
            ctx = null;
            LOG.error("LDAP authentication failed with exception: " + e.getMessage(), e);
        }
        return ctx;
    }

    public final static String CN= "cn";
    public final static String DISPLAY_NAME =  "displayName";
    public final static String DESCRIPTION= "description";
    public final static String SAMACCOUNT_NAME= "sAMAccountName";
    public final static String TELEPHONE_NUMBER= "telephonenumber";
    public final static String GIVEN_NAME= "givenName";
    public final static String UID_NUMBER =  "uidNumber";
    public final static String L = "l";
    public final static String ST = "st";
    public final static String CO = "co";
    public final static String MEMBER_OF = "memberof";
    public final static String SN =  "sn";
    public final static String MAIL = "mail";
    public final static String DISTINGUISHED_NAME =  "distinguishedName";

    protected SearchControls getSearchControl() {
        SearchControls sc = new SearchControls();

        String[] attributeFilter = new String[15];
        attributeFilter[0] = CN;
        attributeFilter[1] =  DISPLAY_NAME ;
        attributeFilter[2] = DESCRIPTION;
        attributeFilter[3] =  SAMACCOUNT_NAME;
        attributeFilter[4] =  TELEPHONE_NUMBER;
        attributeFilter[5] = GIVEN_NAME;
        attributeFilter[6] = UID_NUMBER;
        attributeFilter[7] = L;
        attributeFilter[8] = ST;
        attributeFilter[9] =CO;
        attributeFilter[10] = MEMBER_OF;
        attributeFilter[11] = SN;
        attributeFilter[12] = MAIL;
        attributeFilter[13] = DISTINGUISHED_NAME;

        sc.setReturningAttributes(attributeFilter);
        sc.setSearchScope(SearchControls.SUBTREE_SCOPE);

        return sc;
    }

    public Map<String, String> getUserInfo(String userName) {
        Map<String, String> infos = null;
        for (int i = 0; i < ldapSrvs.size(); i++) {
            if(LOG.isDebugEnabled()) LOG.debug("Using server: "+ldapSrvs.get(i));
            infos = getUserInfo(i, userName);
            if (infos.size() > 0)
                break;
        }
        return infos;
    }

    public Map<String, String> getUserInfo(int id, String userName) {
        if(LOG.isDebugEnabled()) LOG.debug("Ldap get user information for id:"+id+", username:"+userName);
        DirContext ctx = getDirContext(id);
        Map<String, String> infos = new HashMap<String, String>();

        if (ctx != null) {
            try {
                SearchControls sc = getSearchControl();
                String filter = "(&(objectClass=user)(sAMAccountName=" + userName + "))";
                NamingEnumeration<?> results = ctx.search("OU=Accounts_User,DC=corp,DC=company1,DC=com", filter, sc);

                while (results.hasMore()) {
                    SearchResult sr = (SearchResult) results.next();
                    Attributes attrs = sr.getAttributes();

                    for (NamingEnumeration<?> ae = attrs.getAll(); ae.hasMoreElements();) {
                        Attribute attr = (Attribute) ae.next();
                        String attrId = attr.getID();
                        for (NamingEnumeration<?> vals = attr.getAll(); vals.hasMore();) {
                            String thing = vals.next().toString();
                            infos.put(attrId, thing);
                        }
                    }
                }
            } catch (NamingException e) {
                LOG.error("LDAP authentication failed with exception: "+e.getMessage(),e);
            }
        }

        if(LOG.isDebugEnabled()) LOG.debug(infos.toString());
        return infos;
    }

    public boolean authenticate(String userName, String password) {
        for (int i = 0; i < ldapSrvs.size(); i++) {
            if (authenticate(i, userName, password))
                return true;
        }
        return false;
    }

    public boolean authenticate(int id, String userName, String password) {
        boolean result = false;

        DirContext ctx = getDirContext(id);
        if (ctx != null) {
            try {
                SearchControls sc = getSearchControl();
                String filter = "(&(objectClass=user)(sAMAccountName=" + userName + "))";
                NamingEnumeration<?> results = ctx.search("OU=Accounts_User,DC=corp,DC=company1,DC=com", filter, sc);

                String userDN = null;
                if (results.hasMore()) {
                    while (results.hasMore()) {
                        SearchResult sr = (SearchResult) results.next();
                        Attributes attrs = sr.getAttributes();

                        userDN = attrs.get("distinguishedName").get().toString();
                    }
                }
                ctx.close();

                if (userDN != null) {
                    Hashtable<String, String> uenv = new Hashtable<String, String>();
//					if (ldapCerts != null) {
                    uenv.put(Context.SECURITY_PROTOCOL, "ssl");
//					}
                    uenv.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
                    uenv.put(Context.PROVIDER_URL, ldapSrvs.get(id));
                    uenv.put(Context.SECURITY_AUTHENTICATION, "simple");
                    uenv.put(Context.SECURITY_PRINCIPAL, userDN);
                    uenv.put(Context.SECURITY_CREDENTIALS, password);
                    uenv.put("java.naming.ldap.factory.socket","hadoop.eagle.common.service.TrustAllSSLSocketFactory");
                    DirContext uctx = new InitialDirContext(uenv);
                    uctx.close();

                    result = true;
                }
            } catch (NamingException e) {
                LOG.error("LDAP authentication failed with exception: " + e.getMessage(), e);
            }
        }

        return result;
    }
}