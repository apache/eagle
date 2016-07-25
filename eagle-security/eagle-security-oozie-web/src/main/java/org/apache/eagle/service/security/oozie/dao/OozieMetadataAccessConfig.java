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

    public final static class OOZIECONF {
        public final static String ACCESSTYPE = "accessType";
        public final static String OOZIEURL = "oozieUrl";
        public final static String FILTER = "filter";
        public final static String AUTHTYPE = "authType";
    }

    public static OozieMetadataAccessConfig config2Entity(Config config) {
        OozieMetadataAccessConfig oozieconf = new OozieMetadataAccessConfig();
        if(config.hasPath(OOZIECONF.ACCESSTYPE)) {
            oozieconf.setAccessType(config.getString(OOZIECONF.ACCESSTYPE));
        }
        if(config.hasPath(OOZIECONF.OOZIEURL)) {
            oozieconf.setOozieUrl(config.getString(OOZIECONF.OOZIEURL));
        }
        if(config.hasPath(OOZIECONF.FILTER)) {
            oozieconf.setFilter(config.getString(OOZIECONF.FILTER));
        }
        if(config.hasPath(OOZIECONF.AUTHTYPE)) {
            oozieconf.setAuthType(config.getString(OOZIECONF.AUTHTYPE));
        }
        return oozieconf;
    }
}
