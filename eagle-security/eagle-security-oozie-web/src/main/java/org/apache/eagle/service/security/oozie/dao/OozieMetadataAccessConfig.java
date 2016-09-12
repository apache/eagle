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
package org.apache.eagle.service.security.oozie.dao;

import com.typesafe.config.Config;

public class OozieMetadataAccessConfig {
    private String accessType;
    private String oozieUrl;
    private String filter;
    private String authType;

    public String getAccessType() {
        return accessType;
    }

    public void setAccessType(String accessType) {
        this.accessType = accessType;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getAuthType() {
        return authType;
    }

    public void setAuthType(String authType) {
        this.authType = authType;
    }

    public String getOozieUrl() {
        return oozieUrl;
    }

    public void setOozieUrl(String oozieUrl) {
        this.oozieUrl = oozieUrl;
    }

    @Override
    public String toString() {
        return "accessType:" + accessType + ",oozieUrl:" + oozieUrl + ",filter:" + filter + ",authType:" + authType;
    }

    public static final class OozieConf {
        public static final String ACCESSTYPE = "accessType";
        public static final String OOZIEURL = "oozieUrl";
        public static final String FILTER = "filter";
        public static final String AUTHTYPE = "authType";
    }

    public static OozieMetadataAccessConfig config2Entity(Config config) {
        OozieMetadataAccessConfig oozieconf = new OozieMetadataAccessConfig();
        if (config.hasPath(OozieConf.ACCESSTYPE)) {
            oozieconf.setAccessType(config.getString(OozieConf.ACCESSTYPE));
        }
        if (config.hasPath(OozieConf.OOZIEURL)) {
            oozieconf.setOozieUrl(config.getString(OozieConf.OOZIEURL));
        }
        if (config.hasPath(OozieConf.FILTER)) {
            oozieconf.setFilter(config.getString(OozieConf.FILTER));
        }
        if (config.hasPath(OozieConf.AUTHTYPE)) {
            oozieconf.setAuthType(config.getString(OozieConf.AUTHTYPE));
        }
        return oozieconf;
    }
}
