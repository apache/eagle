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

import org.apache.eagle.ml.model.MLCallbackResult;
import org.apache.eagle.security.userprofile.UserProfileConstants;
import org.apache.eagle.security.userprofile.model.UserActivityAggModel;
import org.apache.eagle.security.userprofile.model.UserCommandStatistics;
import org.apache.eagle.security.userprofile.model.UserProfileKDEModel;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.eagle.security.userprofile.UserProfileConstants;
import org.apache.eagle.security.userprofile.model.UserActivityAggModel;
import org.apache.eagle.security.userprofile.model.UserCommandStatistics;
import org.apache.eagle.security.userprofile.model.UserProfileKDEModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserProfileAnomalyKDEEvaluator extends AbstractUserProfileKDEEvaluator{
    private static Logger LOG = LoggerFactory.getLogger(UserProfileAnomalyKDEEvaluator.class);

    @Override
    public List<MLCallbackResult> detect(final String user, final String algorithm, UserActivityAggModel userActivity, UserProfileKDEModel aModel) {
        List<MLCallbackResult> mlPredictionOutputList = new ArrayList<MLCallbackResult>();
        RealMatrix inputData = userActivity.matrix();

        double[] probabilityEstimation = new double[inputData.getRowDimension()];
        for(int i=0; i < probabilityEstimation.length; i++)
            probabilityEstimation[i] = 1.0;

        boolean[][] anomalyFeature = new boolean[inputData.getRowDimension()][inputData.getColumnDimension()];

        for(int i=0; i < anomalyFeature.length;i++){
            for(int j=0; j < anomalyFeature[i].length; j++){
                anomalyFeature[i][j] = false;
            }
        }

        if(aModel == null){
            LOG.info("No model available for this uer, returning");
            return null;
        }

        Map<String,String> context = new HashMap<String,String>(){{
            put(UserProfileConstants.USER_TAG,user);
            put(UserProfileConstants.ALGORITHM_TAG,algorithm);
        }};

        for(int i=0; i<inputData.getRowDimension();i++){
            List<String> cmds = JavaConversions.seqAsJavaList(userActivity.cmdTypes());
            if(inputData.getColumnDimension() != cmds.size()){
                LOG.error("Test data is not with same dimension as training, aborting...");
                return null;
            }else{

                UserCommandStatistics[] listStats = aModel.statistics();

                for(int j=0; j < inputData.getColumnDimension(); j++){
//                    LOG.info("mean for j=" + j + " is:" + listStats[j].getMean());
//                    LOG.info("stddev for j=" + j + " is:" + listStats[j].getStddev());
                    if(listStats[j].isLowVariant()){
//                        LOG.info(listStats[j].getCommandName() + " is low variant for user: " + user);
                        if(inputData.getEntry(i, j) > listStats[j].getMean()){
                            probabilityEstimation[i]*= Double.NEGATIVE_INFINITY;
                            anomalyFeature[i][j] = true;
                        }
                    }else{
                        double stddev = listStats[j].getStddev();
                        //LOG.info("stddev: " + stddev);
                        double mean = listStats[j].getMean();
                        //LOG.info("mean: " + mean);
                        double sqrt2PI = Math.sqrt(2.0*Math.PI);
                        //LOG.info("sqrt2PI: " + sqrt2PI);
                        double denominatorFirstPart = sqrt2PI*stddev;
                        //LOG.info("denominatorFirstPart: " + denominatorFirstPart);
                        double squareMeanNormal = Math.pow((inputData.getEntry(i, j) - mean), 2);
                        //LOG.info("squareMeanNormal: " + squareMeanNormal);
                        double twoPowStandardDev = Math.pow(stddev, 2);
                        //LOG.info("twoPowStandardDev: " + twoPowStandardDev);
                        double twoTimesTwoPowStandardDev = 2.0*twoPowStandardDev;
                        //LOG.info("twoTimesTwoPowStandardDev: " + twoTimesTwoPowStandardDev);

                        double tempVal = ((1.00/denominatorFirstPart)
                                *(Math.exp(-(squareMeanNormal/twoTimesTwoPowStandardDev))));
                        probabilityEstimation[i] *= tempVal;
                        //LOG.info("probabilityEstimation: " + probabilityEstimation[i]);
                        if((inputData.getEntry(i, j) - mean) > 2*stddev)
                            anomalyFeature[i][j] = true;
                    }
                }

            }
        }

        for(int i=0; i < probabilityEstimation.length;i++){
            MLCallbackResult callBackResult = new MLCallbackResult();
            callBackResult.setContext(context);
            //LOG.info("probability estimation for data @" + i + " is: " + probabilityEstimation[i]);
            if(probabilityEstimation[i] < aModel.maxProbabilityEstimate()){
                callBackResult.setAnomaly(true);
                for(int col = 0 ; col < anomalyFeature[i].length; col++){
                    //LOG.info("feature anomaly? " + (featureVals[col] == true));
                    if(anomalyFeature[i][col] == true){
                        callBackResult.setFeature(aModel.statistics()[col].getCommandName());
                    }
                }
            }else{
                callBackResult.setAnomaly(false);
            }

            callBackResult.setTimestamp(userActivity.timestamp());
            List<String> datapoints = new ArrayList<String>();
            double[] rowVals = userActivity.matrix().getRow(i);
            for(double rowVal:rowVals)
                datapoints.add(rowVal+"");
            callBackResult.setDatapoints(datapoints);
            callBackResult.setId(user);
            callBackResult.setAlgorithm(UserProfileConstants.KDE_ALGORITHM);
            mlPredictionOutputList.add(callBackResult);
        }
        return mlPredictionOutputList;
    }
}