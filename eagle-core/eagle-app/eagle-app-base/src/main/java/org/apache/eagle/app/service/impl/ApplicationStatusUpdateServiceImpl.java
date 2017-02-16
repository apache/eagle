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
package org.apache.eagle.app.service.impl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.eagle.app.service.ApplicationOperations;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.metadata.service.ApplicationStatusUpdateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

@Singleton
public class ApplicationStatusUpdateServiceImpl extends ApplicationStatusUpdateService {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationStatusUpdateServiceImpl.class);
    private final ApplicationEntityService applicationEntityService;
    private final ApplicationManagementServiceImpl applicationManagementService;

    // default value 30, 30
    private int initialDelay = 30;
    private int period = 30;


    @Inject
    public ApplicationStatusUpdateServiceImpl(ApplicationEntityService applicationEntityService, ApplicationManagementServiceImpl applicationManagementService) {
        this.applicationEntityService = applicationEntityService;
        this.applicationManagementService = applicationManagementService;
    }

    @Override
    protected void runOneIteration() throws Exception {
        LOG.info("Updating application status");
        try {
            Collection<ApplicationEntity> applicationEntities = applicationEntityService.findAll();
            if (applicationEntities.size() == 0) {
                LOG.info("No application installed yet");
                return;
            }
            for (ApplicationEntity applicationEntity : applicationEntities) {
                if (applicationEntity.getDescriptor().isExecutable()) {
                    updateApplicationEntityStatus(applicationEntity);
                }
            }
            LOG.info("Updated {} application status", applicationEntities.size());
        } catch (Exception e) {
            LOG.error("Failed to update application status", e);
        }
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(initialDelay, period, TimeUnit.SECONDS);
    }

    @Override
    public void updateApplicationEntityStatus(ApplicationEntity applicationEntity) {
        String appUuid = applicationEntity.getUuid();
        ApplicationEntity.Status preStatus = applicationEntity.getStatus();
        try {
            ApplicationEntity.Status currentStatus = applicationManagementService.getStatus(new ApplicationOperations.CheckStatusOperation(appUuid));
            if (preStatus == ApplicationEntity.Status.STARTING) {
                if (currentStatus == ApplicationEntity.Status.RUNNING) {
                    // applicationEntityService.delete(applicationEntity);
                    // applicationEntity.setStatus(ApplicationEntity.Status.RUNNING);
                    // applicationEntityService.create(applicationEntity);
                    currentStatus = ApplicationEntity.Status.RUNNING;
                    // handle the topology corruption case:
                } else if (currentStatus == ApplicationEntity.Status.REMOVED) {
                    // applicationEntityService.delete(applicationEntity);
                    // applicationEntity.setStatus(ApplicationEntity.Status.INITIALIZED);
                    // applicationEntityService.create(applicationEntity);
                    currentStatus = ApplicationEntity.Status.INITIALIZED;
                }
            } else if (preStatus == ApplicationEntity.Status.STOPPING) {
                if (currentStatus == ApplicationEntity.Status.REMOVED) {
                    // applicationEntityService.delete(applicationEntity);
                    // applicationEntity.setStatus(ApplicationEntity.Status.INITIALIZED);
                    // applicationEntityService.create(applicationEntity);
                    currentStatus = ApplicationEntity.Status.INITIALIZED;
                }
            } else if (preStatus == ApplicationEntity.Status.RUNNING) {
                // handle the topology corruption case:
                if (currentStatus == ApplicationEntity.Status.REMOVED) {
                    // applicationEntityService.delete(applicationEntity);
                    // applicationEntity.setStatus(ApplicationEntity.Status.INITIALIZED);
                    // applicationEntityService.create(applicationEntity);
                    currentStatus = ApplicationEntity.Status.INITIALIZED;
                }
            } else if (preStatus == ApplicationEntity.Status.INITIALIZED) {
                //corner case: when Storm service go down, app status-> initialized,
                //then when storm server is up again, storm topology will be launched automatically->active
                if (currentStatus == ApplicationEntity.Status.RUNNING) {
                    // applicationEntityService.delete(applicationEntity);
                    // applicationEntity.setStatus(ApplicationEntity.Status.RUNNING);
                    // applicationEntityService.create(applicationEntity);
                    currentStatus = ApplicationEntity.Status.RUNNING;
                }
            }

            if (currentStatus == ApplicationEntity.Status.REMOVED) {
                currentStatus = ApplicationEntity.Status.INITIALIZED;
            }

            // "STOPPED" is not used in Eagle, so just do nothing.
            if (preStatus != currentStatus) {
                LOG.info("Application {} status changed from {} to {}", applicationEntity.getAppId(), preStatus, currentStatus);
            }
            applicationEntity.setStatus(currentStatus);
            applicationEntityService.update(applicationEntity);
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
        }
    }
}