/*
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

package org.apache.eagle.jpm.analyzer.resource;

import com.google.inject.Inject;
import org.apache.eagle.common.rest.RESTResponse;
import org.apache.eagle.jpm.analyzer.meta.MetaManagementService;
import org.apache.eagle.jpm.analyzer.meta.model.UserEmailEntity;
import org.apache.eagle.jpm.analyzer.meta.model.JobMetaEntity;
import org.apache.eagle.jpm.analyzer.util.Constants;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

import static org.apache.eagle.jpm.analyzer.util.Constants.*;

@Path(ANALYZER_PATH)
public class AnalyzerResource {
    @Inject
    MetaManagementService metaManagementService;

    public AnalyzerResource() {
    }

    @POST
    @Path(JOB_META_ROOT_PATH)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Void> addJobMeta(JobMetaEntity jobMetaEntity) {
        return RESTResponse.<Void>async((response) -> {
            jobMetaEntity.ensureDefault();
            boolean ret = metaManagementService.addJobMeta(jobMetaEntity);
            String message = "Successfully add job meta for " + jobMetaEntity.getSiteId() + ": " + jobMetaEntity.getJobDefId();
            if (!ret) {
                message = "Failed to add job meta for " + jobMetaEntity.getSiteId() + ": " + jobMetaEntity.getJobDefId();
            }
            response.success(ret).message(message);
        }).get();
    }

    @POST
    @Path(JOB_META_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Void> updateJobMeta(@PathParam(Constants.SITE_ID) String siteId,
                                            @PathParam(Constants.JOB_DEF_ID) String jobDefId,
                                            JobMetaEntity jobMetaEntity) {
        return RESTResponse.<Void>async((response) -> {
            jobMetaEntity.setModifiedTime(System.currentTimeMillis());
            jobMetaEntity.setSiteId(siteId);
            jobMetaEntity.setJobDefId(jobDefId);
            boolean ret = metaManagementService.updateJobMeta(jobMetaEntity);
            String message = "Successfully update job meta for " + siteId + ":" + jobDefId;
            if (!ret) {
                message = "Failed to update job meta for " + siteId + ":" + jobDefId;
            }
            response.success(ret).message(message);
        }).get();
    }

    @GET
    @Path(JOB_META_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<List<JobMetaEntity>> getJobMeta(@PathParam(Constants.SITE_ID) String siteId,
                                                        @PathParam(Constants.JOB_DEF_ID) String jobDefId) {
        return RESTResponse.async(() -> metaManagementService.getJobMeta(siteId, jobDefId)).get();
    }

    @DELETE
    @Path(JOB_META_PATH)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Void> deleteJobMeta(@PathParam(Constants.SITE_ID) String siteId,
                                            @PathParam(Constants.JOB_DEF_ID) String jobDefId) {
        return RESTResponse.<Void>async((response) -> {
            boolean ret = metaManagementService.deleteJobMeta(siteId, jobDefId);
            String message = "Successfully delete job meta for " + siteId + ": " + jobDefId;
            if (!ret) {
                message = "Failed to delete job meta for " + siteId + ": " + jobDefId;
            }

            response.success(ret).message(message);
        }).get();
    }

    @POST
    @Path(USER_META_ROOT_PATH)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Void> addEmailPublisherMeta(UserEmailEntity userEmailEntity) {
        return RESTResponse.<Void>async((response) -> {
            userEmailEntity.ensureDefault();
            boolean ret = metaManagementService.addUserEmailMeta(userEmailEntity);
            String message = "Successfully add user meta for " + userEmailEntity.getSiteId() + ": " + userEmailEntity.getUserId();
            if (!ret) {
                message = "Failed to add user meta for " + userEmailEntity.getSiteId() + ": " + userEmailEntity.getUserId();
            }
            response.success(ret).message(message);
        }).get();
    }

    @POST
    @Path(USER_META_PATH)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Void> updateEmailPublisherMeta(@PathParam(Constants.SITE_ID) String siteId,
                                                       @PathParam(Constants.USER_ID) String userId,
                                                       UserEmailEntity userEmailEntity) {
        return RESTResponse.<Void>async((response) -> {
            userEmailEntity.setSiteId(siteId);
            userEmailEntity.setUserId(userId);
            userEmailEntity.setModifiedTime(System.currentTimeMillis());
            boolean ret = metaManagementService.updateUserEmailMeta(userEmailEntity);
            String message = "Successfully add user meta for " + userEmailEntity.getSiteId() + ": " + userEmailEntity.getUserId();
            if (!ret) {
                message = "Failed to add user meta for " + userEmailEntity.getSiteId() + ": " + userEmailEntity.getUserId();
            }
            response.success(ret).message(message);
        }).get();
    }

    @DELETE
    @Path(USER_META_PATH)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<Void> deleteEmailPublisherMeta(@PathParam(Constants.SITE_ID) String siteId,
                                                       @PathParam(Constants.USER_ID) String userId) {
        return RESTResponse.<Void>async((response) -> {
            boolean ret = metaManagementService.deleteUserEmailMeta(siteId, userId);
            String message = "Successfully delete user meta for " + siteId + ":" + userId;
            if (!ret) {
                message = "Failed to delete user meta for " + siteId + ":" + userId;
            }
            response.success(ret).message(message);
        }).get();
    }

    @GET
    @Path(USER_META_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RESTResponse<List<UserEmailEntity>> getEmailPublisherMeta(@PathParam(Constants.SITE_ID) String siteId,
                                                                     @PathParam(Constants.USER_ID) String userId) {
        return RESTResponse.async(() -> metaManagementService.getUserEmailMeta(siteId, userId)).get();
    }
}
