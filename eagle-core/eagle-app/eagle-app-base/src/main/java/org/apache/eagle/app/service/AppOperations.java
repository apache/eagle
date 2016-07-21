/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.service;

import org.apache.eagle.metadata.model.ApplicationEntity;

import java.io.Serializable;
import java.util.Map;

public class AppOperations {
    interface Operation extends Serializable {
        //
    }

    public static class InstallOperation implements Operation{
        private String siteId;
        private String appType;
        private ApplicationEntity.Mode mode = ApplicationEntity.Mode.LOCAL;
        private Map<String,Object> configuration;

        public InstallOperation(){}
        public InstallOperation(String siteId,String appType){
            this.setSiteId(siteId);
            this.setAppType(appType);
        }
        public InstallOperation(String siteId,String appType,ApplicationEntity.Mode mode){
            this.setSiteId(siteId);
            this.setAppType(appType);
            this.setMode(mode);
        }
        public InstallOperation(String siteId,String appType,ApplicationEntity.Mode mode,Map<String,Object> configuration){
            this.setSiteId(siteId);
            this.setAppType(appType);
            this.setMode(mode);
            this.setConfiguration(configuration);
        }

        public String getSiteId() {
           return siteId;
       }
        public void setSiteId(String siteId) {
           this.siteId = siteId;
       }
        public String getAppType() {
           return appType;
       }
        public void setAppType(String appType) {
           this.appType = appType;
       }

        public Map<String, Object> getConfiguration() {
            return configuration;
        }

        public void setConfiguration(Map<String, Object> configuration) {
            this.configuration = configuration;
        }

        public ApplicationEntity.Mode getMode() {
            return mode;
        }

        public void setMode(ApplicationEntity.Mode mode) {
            this.mode = mode;
        }
    }

    public static class UninstallOperation implements Operation{
        private String uuid;
        private String appId;
        public UninstallOperation(String uuid){
            this.setUuid(uuid);
        }
        public UninstallOperation(String uuid,String appId){
            this.setUuid(uuid);
            this.setAppId(appId);
        }

        public String getUuid() {
            return uuid;
        }
        public void setUuid(String uuid) {
            this.uuid = uuid;
        }

        public String getAppId() {
            return appId;
        }

        public void setAppId(String appId) {
            this.appId = appId;
        }
    }

    public static class StartOperation implements Operation{
        private String uuid;
        private String appId;
        public StartOperation(String uuid){
            this.setUuid(uuid);
        }
        public StartOperation(String uuid,String appId){
            this.setUuid(uuid);
            this.setAppId(appId);
        }
        public String getUuid() {
            return uuid;
        }
        public void setUuid(String uuid) {
            this.uuid = uuid;
        }

        public String getAppId() {
            return appId;
        }

        public void setAppId(String appId) {
            this.appId = appId;
        }
    }

    public static class StopOperation implements Operation{
        private String uuid;
        private String appId;

        public StopOperation(String uuid){
            this.setUuid(uuid);
        }
        public StopOperation(String uuid,String appId){
            this.setUuid(uuid);
            this.setAppId(appId);
        }
        public String getUuid() {
            return uuid;
        }
        public void setUuid(String uuid) {
            this.uuid = uuid;
        }

        public String getAppId() {
            return appId;
        }

        public void setAppId(String appId) {
            this.appId = appId;
        }
    }
}