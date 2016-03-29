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
package org.apache.eagle.service.security.hive.dao;

import com.typesafe.config.Config;

public class HiveMetadataAccessConfig {
    private String accessType;
    private String user;
    private String password;
    private String jdbcDriverClassName;
    private String jdbcUrl;

    public String getAccessType() {
        return accessType;
    }

    public void setAccessType(String accessType) {
        this.accessType = accessType;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getJdbcDriverClassName() {
        return jdbcDriverClassName;
    }

    public void setJdbcDriverClassName(String jdbcDriverClassName) {
        this.jdbcDriverClassName = jdbcDriverClassName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUser() {
        return user;

    }

    public void setUser(String user) {
        this.user = user;
    }

    public String toString(){
        return "user:" + user +",jdbcDriverClassName:" + jdbcDriverClassName + ",jdbcUrl:" + jdbcUrl;
    }

    public final static class HIVECONF {
        public final static String ACCESSTYPE = "accessType";
        public final static String USER = "user";
        public final static String PASSWD = "password";
        public final static String JDBCDRIVER = "jdbcDriverClassName";
        public final static String JDBCURL = "jdbcUrl";
    }

    public static HiveMetadataAccessConfig config2Entity(Config config) {
        HiveMetadataAccessConfig hiveConf = new HiveMetadataAccessConfig();
        if(config.hasPath(HIVECONF.ACCESSTYPE)) {
            hiveConf.setAccessType(config.getString(HIVECONF.ACCESSTYPE));
        }
        if(config.hasPath(HIVECONF.USER)) {
            hiveConf.setUser(config.getString(HIVECONF.USER));
        }
        if(config.hasPath(HIVECONF.PASSWD)) {
            hiveConf.setPassword(config.getString(HIVECONF.PASSWD));
        }
        if(config.hasPath(HIVECONF.JDBCDRIVER)) {
            hiveConf.setJdbcDriverClassName(config.getString(HIVECONF.JDBCDRIVER));
        }
        if(config.hasPath(HIVECONF.JDBCURL)) {
            hiveConf.setJdbcUrl(config.getString(HIVECONF.JDBCURL));
        }
        return hiveConf;
    }
}


