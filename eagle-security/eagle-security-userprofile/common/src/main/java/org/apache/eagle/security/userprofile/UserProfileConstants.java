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
package org.apache.eagle.security.userprofile;

import org.apache.eagle.ml.MLConstants;

import java.io.Serializable;

public final class UserProfileConstants implements Serializable{
    public static final String[] DEFAULT_CMD_TYPES= new String[]{"getfileinfo", "open", "listStatus", "setTimes", "setPermission", "rename", "mkdirs", "create", "setReplication", "contentSummary", "delete", "setOwner", "fsck"};
    public static final String DEFAULT_TRAINING_APP_NAME = "AuditlogTraining";

    public static final String USER_PROFILE_EIGEN_MODEL_SERVICE = "UserProfileEigenModelService";
    public static final String USER_PROFILE_KDE_MODEL_SERVICE = "UserProfileKDEModelService";
    public static final String USER_ACTIVITY_AGG_MODEL_SERVICE = "UserActivityAggModelService";

    public static final String EIGEN_DECOMPOSITION_ALGORITHM = "EigenDecomposition";
    public static final String KDE_ALGORITHM = "DE";

    public static final String USER_TAG = "user";
    public static final String SITE_TAG = "site";
    public static final String ALGORITHM_TAG = MLConstants.ALGORITHM;
}