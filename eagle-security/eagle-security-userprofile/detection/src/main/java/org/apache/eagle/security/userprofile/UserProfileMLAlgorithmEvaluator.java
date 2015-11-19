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

import org.apache.eagle.alert.siddhi.EagleAlertContext;
import org.apache.eagle.dataproc.core.ValuesArray;
import org.apache.eagle.ml.MLAlgorithmEvaluator;
import org.apache.eagle.ml.MLAnomalyCallback;
import org.apache.eagle.ml.MLModelDAO;
import org.apache.eagle.ml.impl.MLModelDAOImpl;
import org.apache.eagle.ml.model.MLAlgorithm;
import org.apache.eagle.ml.model.MLCallbackResult;
import org.apache.eagle.ml.model.MLModelAPIEntity;
import org.apache.eagle.security.userprofile.model.UserActivityAggModelEntity;
import org.apache.eagle.security.userprofile.model.UserProfileModel;
import com.typesafe.config.Config;
import org.apache.eagle.security.userprofile.model.UserActivityAggModelEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class UserProfileMLAlgorithmEvaluator<M extends UserProfileModel> implements MLAlgorithmEvaluator,UserProfileAnomalyDetector<M> {
    protected transient MLAlgorithm algorithm;
    protected transient List<MLAnomalyCallback> callbacks;
    private transient Config config;
    private final static Logger LOG = LoggerFactory.getLogger(UserProfileMLAlgorithmEvaluator.class);
    private transient MLModelDAO mlDAO;
    private transient UserProfileAnomalyDetector<M> detector;

    @Override
    public void init(MLAlgorithm algorithm, Config config) {
        this.algorithm = algorithm;
        this.callbacks = new ArrayList<>();
        this.config = config;
        this.mlDAO = this.getModelDAO();
        this.detector = getAnomalyDetector();
    }

    protected UserProfileAnomalyDetector<M> getAnomalyDetector(){ return this;}

    protected MLModelDAO getModelDAO(){
        // TODO: 1. Adapt to different model protocol including service, hdfs, file or kafka
        // TODO: 2. Model cache configuration
        return new MLModelDAOImpl(this.config);
    }

    /**
     * @param data ValuesArray[0:alertContext{alertExecutor,policyId,evaluator,outputCollector},1:streamName,2:entity]
     *
     * @throws Exception
     */
    @Override
    public void evaluate(ValuesArray data) throws Exception {
        EagleAlertContext alertContext = (EagleAlertContext)data.get(0);
        String streamName = (String)data.get(1);

        UserActivityAggModelEntity userActivityAggModelEntity = (UserActivityAggModelEntity) data.get(2);

        String user = userActivityAggModelEntity.getTags().get(UserProfileConstants.USER_TAG);
        String algorithm = this.algorithm.getName();

        if(algorithm == null) LOG.warn("Algorithm name is null: "+this.algorithm);

        List<MLModelAPIEntity> mlModels = this.mlDAO.findMLModelByContext(user, algorithm);
        M model;
        if(mlModels == null || mlModels.size() == 0){
            LOG.warn(String.format("No persisted ML model found for user [%s], algorithm [%s]",user,algorithm));
        }else{
            if(mlModels.size()>0){
                LOG.warn(String.format("%s models found for user [%s], algorithm [%s]",mlModels.size(),user,algorithm));
            }
            for (MLModelAPIEntity mlModel : mlModels) {
                model = detector.convert(mlModel);
                //LOG.info("executing algorithm: " + );
                List<MLCallbackResult> callbackResults = this.detector.detect(user, algorithm, UserActivityAggModelEntity.toModel(userActivityAggModelEntity), model);
                if (callbackResults != null && callbackResults.size() >0) {
                    notifyCallbacks(callbackResults, alertContext);
                } else {
                    LOG.info(String.format("No anomaly activities detected for user [%s] by algorithm [%s] ",user,algorithm));
                }
            }
        }
    }

    protected void notifyCallbacks(List<MLCallbackResult> callbackResults, EagleAlertContext alertContext){
        for(MLCallbackResult callbackResult: callbackResults){
            if(callbackResult.isAnomaly()) {
                for (MLAnomalyCallback callback : this.callbacks) {
                    callback.receive(callbackResult,alertContext);
                }
            }
        }
    }

    @Override
    public void register(MLAnomalyCallback callback) throws Exception {
        this.callbacks.add(callback);
    }
}