/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */
package org.apache.eagle.contrib.connector.webservice;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * PolicyQeuryParams is mainly for construct a query parameters in URL
 * Eg: 1. @application="hdfsAuditLog"
 *     2. @applicaiton="hdfsAuditLog" and @site="sandbox"
 *     3. @applicaiton="hdfsAuditLog" and @site="sandbox" and @policyId="test"
 * */
public class PolicyQueryParams {
    private HashMap<String, String> params = new HashMap<String, String>();

    public String getApplication(){
        return params.get("application");
    }
    public void setApplication(String application){
        params.put("application",application);
    }


    public String getSite() {
        return params.get("site");
    }

    public void setSite(String site) {
        params.put("site", site);

    }

    public String getPolicyId() {
        return params.get("policyId");
    }

    public void setPolicyId(String policyId) {
        params.put("policyId",policyId);
    }

    /**
     * prepare params for url creation, eg: @applicaiton="hdfsAuditLog" and @site="sandbox" and @policyId="test"
     * */
    public String getParams(){
        String parameters = "";
        if(params.size() == 0) return parameters;
        Iterator it = params.entrySet().iterator();
        if(params.size() == 1) {
            Map.Entry<String,String> pair = (Map.Entry<String,String>) it.next();
            return "@" + pair.getKey() + "=" + "\"" + pair.getValue() + "\"";
        }
        else{
            while(it.hasNext()){
                Map.Entry<String,String> pair = (Map.Entry<String,String>) it.next();
                parameters += "@" + pair.getKey() + "=" + "\"" + pair.getValue() + "\"";
                if(it.hasNext()) parameters += " and ";
            }
            return parameters;
        }
    }
}
