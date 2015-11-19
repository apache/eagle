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
import org.apache.eagle.security.userprofile.model.UserProfileEigenModel;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.eagle.security.userprofile.UserProfileConstants;
import org.apache.eagle.security.userprofile.model.UserProfileEigenModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserProfileAnomalyEigenEvaluator extends AbstractUserProfileEigenEvaluator {
    private static Logger LOG = LoggerFactory.getLogger(UserProfileAnomalyEigenEvaluator.class);
    private RealMatrix normalizeData(RealMatrix matrix, UserProfileEigenModel model){
        RealMatrix normalizedData = new Array2DRowRealMatrix(matrix.getRowDimension(), matrix.getColumnDimension());
        if(LOG.isDebugEnabled()) LOG.debug("model statistics size: " + model.statistics().length);
        for(int i=0; i< matrix.getRowDimension(); i++){
            for(int j=0; j < matrix.getColumnDimension(); j++){
                double value = (matrix.getEntry(i, j) - model.statistics()[j].getMean())/model.statistics()[j].getStddev();
                normalizedData.setEntry(i, j, value);
            }
        }
        return normalizedData;
    }

    @Override
    public List<MLCallbackResult> detect(final String user, final String algorithm, UserActivityAggModel userActivity, UserProfileEigenModel aModel) {
        RealMatrix inputData = userActivity.matrix();
        LOG.warn("EigenBasedAnomalyDetection predictAnomaly called with dimension: " + inputData.getRowDimension() + "x" + inputData.getColumnDimension());

        if(aModel== null){
            LOG.warn("nothing to do as the input model does not have required values, returning from evaluating this algorithm..");
            return null;
        }

        List<MLCallbackResult> mlCallbackResults = new ArrayList<MLCallbackResult>();
        RealMatrix normalizedMat = normalizeData(inputData, aModel);

        UserCommandStatistics[] listStats = aModel.statistics();
        int colWithHighVariant = 0;

        for (int j = 0; j < normalizedMat.getColumnDimension(); j++) {
            if (listStats[j].isLowVariant() == false) {
                colWithHighVariant++;
            }
        }

        final Map<String,String> context = new HashMap<String, String>(){{
            put(UserProfileConstants.USER_TAG,user);
            put(UserProfileConstants.ALGORITHM_TAG,algorithm);
        }};

        Map<Integer, String> lineNoWithVariantBasedAnomalyDetection = new HashMap<Integer, String>();
        for (int i = 0; i < normalizedMat.getRowDimension(); i++) {
            MLCallbackResult aResult = new MLCallbackResult();
            aResult.setAnomaly(false);
            aResult.setContext(context);

            for (int j = 0; j < normalizedMat.getColumnDimension(); j++) {
                //LOG.info("mean for j=" + j + " is:" + listStats[j].getMean());
                //LOG.info("stddev for j=" + j + " is:" + listStats[j].getStddev());
                if (listStats[j].isLowVariant() == true) {
                    //LOG.info(listOfCmds[j] + " is low variant");
                    if (normalizedMat.getEntry(i, j) > listStats[j].getMean()) {
                        lineNoWithVariantBasedAnomalyDetection.put(i, "lowVariantAnomaly");
                        aResult.setAnomaly(true);
                        aResult.setTimestamp(userActivity.timestamp());
                        aResult.setFeature(listStats[j].getCommandName());
                        aResult.setAlgorithm(UserProfileConstants.EIGEN_DECOMPOSITION_ALGORITHM);
                        List<String> datapoints = new ArrayList<String>();
                        double[] rowVals = inputData.getRow(i);
                        for(double rowVal:rowVals)
                            datapoints.add(rowVal+"");
                        aResult.setDatapoints(datapoints);
                        aResult.setId(user);
                        //mlCallbackResults.add(aResult);
                    } /*else {
                        aResult.setAnomaly(false);
                        aResult.setTimestamp(userActivity.timestamp());
                        mlCallbackResults.add(aResult);
                    }*/
                }
            }
            mlCallbackResults.add(i, aResult);
            //return results;
        }

        //LOG.info("results size here: " + results.length);

        //LOG.info("col with high variant: " + colWithHighVariant);
        RealMatrix finalMatWithoutLowVariantFeatures = new Array2DRowRealMatrix(normalizedMat.getRowDimension(), colWithHighVariant);
        //LOG.info("size of final test data: " + finalMatWithoutLowVariantFeatures.getRowDimension() +"x"+ finalMatWithoutLowVariantFeatures.getColumnDimension());
        int finalMatrixRow = 0;
        int finalMatrixCol = 0;
        for (int i = 0; i < normalizedMat.getRowDimension(); i++) {
            for (int j = 0; j < normalizedMat.getColumnDimension(); j++) {
                if (listStats[j].isLowVariant() == false) {
                    finalMatWithoutLowVariantFeatures.setEntry(finalMatrixRow, finalMatrixCol, normalizedMat.getEntry(i, j));
                    finalMatrixCol++;
                }
            }
            finalMatrixCol = 0;
            finalMatrixRow++;
        }
        RealVector[] pcs = aModel.principalComponents();
        //LOG.info("pc size: " + pcs.getRowDimension() +"x" + pcs.getColumnDimension());

        RealMatrix finalInputMatTranspose = finalMatWithoutLowVariantFeatures.transpose();

        for (int i = 0; i < finalMatWithoutLowVariantFeatures.getRowDimension(); i++) {
            if (lineNoWithVariantBasedAnomalyDetection.get(i) == null) {
                //MLCallbackResult result = new MLCallbackResult();
                MLCallbackResult result = mlCallbackResults.get(i);
                //result.setContext(context);
                for (int sz = 0; sz < pcs.length; sz++) {
                    double[] pc1 = pcs[sz].toArray();
                    RealMatrix pc1Mat = new Array2DRowRealMatrix(pc1);
                    RealMatrix transposePC1Mat = pc1Mat.transpose();
                    RealMatrix testData = pc1Mat.multiply(transposePC1Mat).multiply(finalInputMatTranspose.getColumnMatrix(i));
                    //LOG.info("testData size: " + testData.getRowDimension() + "x" + testData.getColumnDimension());
                    RealMatrix testDataTranspose = testData.transpose();
                    //LOG.info("testData transpose size: " + testDataTranspose.getRowDimension() + "x" + testDataTranspose.getColumnDimension());
                    RealVector iRowVector = testDataTranspose.getRowVector(0);
                    //RealVector pc1Vector = transposePC1Mat.getRowVector(sz);
                    RealVector pc1Vector = transposePC1Mat.getRowVector(0);
                    double distanceiRowAndPC1 = iRowVector.getDistance(pc1Vector);
                    //LOG.info("distance from pc sz: " + sz + " " + distanceiRowAndPC1 + " " + model.getMaxL2Norm().getEntry(sz));
                    //LOG.info("model.getMaxL2Norm().getEntry(sz):" + model.getMaxL2Norm().getEntry(sz));
                    if (distanceiRowAndPC1 > aModel.maximumL2Norm().getEntry(sz)) {
                        //LOG.info("distance from pc sz: " + sz + " " + distanceiRowAndPC1 + " " + model.getMaxL2Norm().getEntry(sz));
                        result.setAnomaly(true);
                        result.setFeature(aModel.statistics()[sz].getCommandName());
                        result.setTimestamp(System.currentTimeMillis());
                        result.setAlgorithm(UserProfileConstants.EIGEN_DECOMPOSITION_ALGORITHM);
                        List<String> datapoints = new ArrayList<String>();
                        double[] rowVals = inputData.getRow(i);
                        for(double rowVal:rowVals)
                            datapoints.add(rowVal+"");
                        result.setDatapoints(datapoints);
                        result.setId(user);
                    }
                }
                mlCallbackResults.set(i, result);
            }
        }
        return mlCallbackResults;
    }
}