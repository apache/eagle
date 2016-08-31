/**
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
package org.apache.eagle.metadata.service.memory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.metadata.service.ApplicationStatusUpdateService;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.eagle.metadata.utils.HttpRequest.httpGetWithoutCredentials;

@Singleton
public class ApplicationStatusUpdateServiceImpl extends  ApplicationStatusUpdateService {
    private final static Logger LOG = LoggerFactory.getLogger(ApplicationStatusUpdateServiceImpl.class);

    private  String stormUrl;
    private  int initialDelay;
    private  int period;

    private  ApplicationEntityService applicationEntityService;

    private  Map<String,ApplicationEntity.Status> appStatusMap;

    private void extractStatus(JSONObject response) {
        try{
            JSONArray list = (JSONArray) response.get("topologies");
            for(int i = 0; i< list.length();i++ ){
                JSONObject topology = (JSONObject) list.get(i);
                String topologyId = topology.getString("id");
                String topologyStatus = (topology.getString("status")).toUpperCase();
                // storm rest api call will return "active" if existed
                if(topologyStatus.toUpperCase().equals("ACTIVE")) {
                    appStatusMap.put(topologyId, ApplicationEntity.Status.RUNNING);
                }
                else if(topologyStatus.toUpperCase().equals("INACTIVE")){
                    appStatusMap.put(topologyId, ApplicationEntity.Status.STOPPED);
                }
                // storm rest api call will return empty if topology get killed
                else {
                    appStatusMap.put(topologyId, ApplicationEntity.Status.STOPPED);

                }
            }
        }catch(Exception e){
            LOG.info("extract application status from Storm failed.");
        }
    }

    @Inject
    public ApplicationStatusUpdateServiceImpl(ApplicationEntityService applicationEntityService){
        this.applicationEntityService = applicationEntityService;
        try{
            Config config = ConfigFactory.load();
            String host = config.getString("application.storm.uiHost");
            String port = config.getString("application.storm.uiPort");
            stormUrl = "http://" + host + ":" + port + "/api/v1/topology/summary";
        } catch (Exception e){
            LOG.info("Failed to get configuration for application updateStatus service, using default value", e);
        }
    }


    @Override
    protected void runOneIteration() throws Exception {
        LOG.info("Checking app status");
        try{
            //fetch all app
            //Todo: now is using mem hashmap, need to adapt to jdbc
            Collection<ApplicationEntity> applicationEntities= applicationEntityService.findAll();
            //fetch status from storm ui
            JSONObject response = httpGetWithoutCredentials(stormUrl);
            //get all status and uid
            extractStatus(response);
            //update one by one
            updateApplicationEntityStatus(applicationEntities);
        }catch (Exception e){
            LOG.info("failed to update app status", e);
        }

    }

    @Override
    protected Scheduler scheduler() {
        //by default use 10, 10
        int initialDelay = 10;
        int period = 10;
        try{
            Config config = ConfigFactory.load();
            initialDelay = config.getInt("application.updateStatus.initialDelay");
            period = config.getInt("application.updateStatus.period");
        } catch (Exception e){
            LOG.info("Failed to get configuration for application updateStatus service, using default value");
        }
        return Scheduler.newFixedRateSchedule(initialDelay, period, TimeUnit.SECONDS);
    }

    @Override
    public void updateApplicationEntityStatus(Collection<ApplicationEntity> applicationEntities){
        for(ApplicationEntity applicationEntity : applicationEntities){
            String appUuid = applicationEntity.getUuid();
            switch (applicationEntity.getStatus()) {
                // only need to consider starting, stopping,
                // there is no chance that app entity  with status "uninstalling"  can appear in storm's topologies.
                case STARTING:{
                    if(appStatusMap.containsKey(appUuid)){
                        ApplicationEntity.Status topologyStatus = (ApplicationEntity.Status) appStatusMap.get(appUuid);
                        if(topologyStatus.equals(ApplicationEntity.Status.RUNNING)) {
                            applicationEntity.setStatus(ApplicationEntity.Status.RUNNING);
                            //update in jdbc
                            applicationEntityService.create(applicationEntity);
                        }//else keep same
                    }
                    break;
                }
                case STOPPING:{
                    if(!appStatusMap.containsKey(appUuid) || ((ApplicationEntity.Status) appStatusMap.get(appUuid)).equals(ApplicationEntity.Status.STOPPED ) ){
                        applicationEntity.setStatus(ApplicationEntity.Status.STOPPED);
                        applicationEntityService.create(applicationEntity);
                    }
                    break;
                }
                //Todo: should tell the difference between inactive and uninstalled
                default:{
                    ;//do nothing
                }
            }
        }
    }
}
