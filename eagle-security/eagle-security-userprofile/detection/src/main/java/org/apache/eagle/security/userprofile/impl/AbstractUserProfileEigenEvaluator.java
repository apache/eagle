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
package org.apache.eagle.security.userprofile.impl;

import org.apache.eagle.ml.model.MLModelAPIEntity;
import org.apache.eagle.security.userprofile.UserProfileMLAlgorithmEvaluator;
import org.apache.eagle.security.userprofile.model.UserProfileEigenModel;
import org.apache.eagle.security.userprofile.UserProfileMLAlgorithmEvaluator;
import org.apache.eagle.security.userprofile.model.UserProfileEigenModel;
import org.apache.eagle.security.userprofile.model.UserProfileEigenModelEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.eagle.security.userprofile.model.UserProfileEigenModelEntity.deserializeModel;

public abstract class AbstractUserProfileEigenEvaluator extends UserProfileMLAlgorithmEvaluator<UserProfileEigenModel> {
    private final static Logger LOG = LoggerFactory.getLogger(AbstractUserProfileEigenEvaluator.class);
    @Override
    public UserProfileEigenModel convert(MLModelAPIEntity entity) throws IOException {
        try {
            UserProfileEigenModel model = UserProfileEigenModelEntity.deserializeModel(entity);
            if(model == null || model.uMatrix() == null || model.diagonalMatrix() == null || model.minVector() == null || model.maxVector() == null
                    || model.maximumL2Norm() == null || model.minimumL2Norm() == null || model.dimension() == 0){
                if(LOG.isDebugEnabled()) LOG.debug("Eigen model seems to not have meaningful information, ignore");
                return null;
            }else {
                return model;
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(),e);
            throw e;
        }
    }
}