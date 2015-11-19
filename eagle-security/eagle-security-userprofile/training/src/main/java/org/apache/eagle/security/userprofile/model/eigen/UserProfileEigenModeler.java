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
package org.apache.eagle.security.userprofile.model.eigen;

import org.apache.eagle.security.userprofile.UserProfileConstants;
import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.stat.correlation.Covariance;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.eagle.security.userprofile.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class UserProfileEigenModeler extends JavaUserProfileModeler<UserProfileEigenModel,UserProfileContext> {

    private final String[] cmdTypes;
    private UserCommandStatistics[] statistics;
    private String outputLocation;
    private RealMatrix finalMatrixWithoutLowVariantCmds;
    private RealMatrix covarianceMatrix;
    private RealMatrix uMatrix;
    private RealMatrix vMatrix;
    private RealMatrix diagonalMatrix;
    private int dimension =0;
    private static final double MAINTENED_VARIANCE = 0.99;
    private RealVector[] principalComponents;
    private RealVector maximumL2Norm; //Double.NEGATIVE_INFINITY;
    private RealVector minimumL2Norm; //= Double.POSITIVE_INFINITY;
    private RealVector minVector;
    private RealVector maxVector;

    private final static Logger LOG = LoggerFactory.getLogger(UserProfileEigenModeler.class);
    private static final double lowVarianceVal= 0.001;

    public RealVector[] getPrincipalComponents() {
        return principalComponents;
    }

    public void setPrincipalComponents(RealVector[] principalComponents) {
        this.principalComponents = principalComponents;
    }

    public UserProfileEigenModeler(String[] cmdTypes){
        this.cmdTypes = cmdTypes;
    }

    public UserProfileEigenModeler(){
        this.cmdTypes = UserProfileConstants.DEFAULT_CMD_TYPES;
    }

    public RealVector getMaximumL2Norm() {
        return maximumL2Norm;
    }

    public void setMaximumL2Norm(RealVector maximumL2Norm) {
        this.maximumL2Norm = maximumL2Norm;
    }

    public RealVector getMinimumL2Norm() {
        return minimumL2Norm;
    }

    public void setMinimumL2Norm(RealVector minimumL2Norm) {
        this.minimumL2Norm = minimumL2Norm;
    }

    public RealVector getMinVector() {
        return minVector;
    }

    public void setMinVector(RealVector minVector) {
        this.minVector = minVector;
    }

    public RealVector getMaxVector() {
        return maxVector;
    }

    public void setMaxVector(RealVector maxVector) {
        this.maxVector = maxVector;
    }

    public UserCommandStatistics[] getStatistics() {
        return statistics;
    }

    public void setStatistics(UserCommandStatistics[] statistics) {
        this.statistics = statistics;
    }

    public String getOutputLocation() {
        return outputLocation;
    }

    public void setOutputLocation(String outputLocation) {
        this.outputLocation = outputLocation;
    }

    private void computeStats(RealMatrix m){

        if(m.getColumnDimension() != this.cmdTypes.length){
            LOG.error("Please fix the commands list in config file");
            return;
        }
        statistics = new UserCommandStatistics[m.getColumnDimension()];
        for(int i=0; i<m.getColumnDimension(); i++){
            UserCommandStatistics stats = new UserCommandStatistics();
            stats.setCommandName(this.cmdTypes[i]);
            RealVector colData = m.getColumnVector(i);
            StandardDeviation deviation = new StandardDeviation();
            double stddev = deviation.evaluate(colData.toArray());
            //LOG.info("stddev is nan?" + (stddev == Double.NaN? "yes":"no"));
            if(stddev <= lowVarianceVal)
                stats.setLowVariant(true);
            else
                stats.setLowVariant(false);
            stats.setStddev(stddev);
            Mean mean = new Mean();
            double mu = mean.evaluate(colData.toArray());
            //LOG.info("mu is nan?" + (mu == Double.NaN? "yes":"no"));
            stats.setMean(mu);
            statistics[i] = stats;
        }
    }

    private RealMatrix normalizeData(RealMatrix matrix){

        RealMatrix normalizedData = new Array2DRowRealMatrix(matrix.getRowDimension(), matrix.getColumnDimension());
        for(int i=0; i< matrix.getRowDimension(); i++){
            for(int j=0; j < matrix.getColumnDimension(); j++){
                // TODO:  statistics[j].getStddev() == 0 what should the value be if stddev is o?
                double value = statistics[j].getStddev() == 0? 0 : (matrix.getEntry(i, j) - statistics[j].getMean())/statistics[j].getStddev();
                normalizedData.setEntry(i, j, value);
            }
        }
        return normalizedData;
    }

    private void computeCovarianceAndSVD(RealMatrix inputMat, int containsLowVariantCol){

        int finalMatrixRow=0;
        int finalMatrixCol=0;

        LOG.info("containsLowVariantCol size: " + containsLowVariantCol);
        int colDimension = (inputMat.getColumnDimension() - containsLowVariantCol);
        try {
            finalMatrixWithoutLowVariantCmds = new Array2DRowRealMatrix(inputMat.getRowDimension(), colDimension);
        }catch (NotStrictlyPositiveException e){
            LOG.error(String.format("Failed to build matrix [rowDimension:%s, columnDimension: %s]",inputMat.getRowDimension(),colDimension),e);
            throw e;
        }

        for(int i=0; i < inputMat.getRowDimension(); i++){
            for(int j=0; j < inputMat.getColumnDimension();j++){
                if(!statistics[j].isLowVariant()){
                    finalMatrixWithoutLowVariantCmds.setEntry(finalMatrixRow, finalMatrixCol, inputMat.getEntry(i, j));
                    finalMatrixCol++;
                }
            }
            finalMatrixCol = 0;
            finalMatrixRow++;
        }

        Covariance cov;
        try {
            cov = new Covariance(finalMatrixWithoutLowVariantCmds.getData());
        }catch (Exception ex){
            throw new IllegalArgumentException(String.format("Failed to create covariance from matrix [ %s x %s ]",finalMatrixWithoutLowVariantCmds.getRowDimension(),finalMatrixWithoutLowVariantCmds.getColumnDimension()),ex);
        }
        covarianceMatrix = cov.getCovarianceMatrix();
        SingularValueDecomposition svd = new SingularValueDecomposition(covarianceMatrix);
        diagonalMatrix = svd.getS();
        uMatrix = svd.getU();
        vMatrix = svd.getV();
    }

    private void computeDimensionWithMaxVariance(){
        if(diagonalMatrix == null || diagonalMatrix.getRowDimension() != diagonalMatrix.getColumnDimension()){
            LOG.error("Diagonal matrix is not correctly computed, SVD should have been done");
            return;
        }

        double sumTotal = 0.0;
        for(int i=0; i < diagonalMatrix.getRowDimension(); i++){
            sumTotal += diagonalMatrix.getEntry(i, i);
        }
        //LOG.info("sum total: " + sumTotal);
        double sumEach = 0.0;
        for(int i=0; i < diagonalMatrix.getRowDimension(); i++){
            sumEach += diagonalMatrix.getEntry(i, i);
            if((sumEach/sumTotal) >= MAINTENED_VARIANCE){
                dimension = i;
                break;
            }

        }
        if(dimension > finalMatrixWithoutLowVariantCmds.getColumnDimension())
            LOG.error("Reduced dimension cannot be smaller than original size of the feature");

    }

    private void computePrincipalComponents(){
        principalComponents = new ArrayRealVector[uMatrix.getColumnDimension()];
        for(int i=0; i < uMatrix.getColumnDimension(); i++){
            principalComponents[i] = uMatrix.getColumnVector(i).mapMultiply(Math.sqrt(diagonalMatrix.getEntry(i, i)));
        }
    }

    private RealMatrix computeMaxDistanceOnPCs(int index){

        RealMatrix pc = new Array2DRowRealMatrix(principalComponents[index].toArray());
        RealMatrix transposePC1 = pc.transpose();
        RealMatrix finalDataTranspose = finalMatrixWithoutLowVariantCmds.transpose();

        RealMatrix trainingData = pc.multiply(transposePC1).multiply(finalDataTranspose);
        RealMatrix trainingDataTranspose = trainingData.transpose();

        double maxDistance = 0.0;
        double minDistance = 0.0;
        RealVector p1 = null, p2 = null;
        if(LOG.isDebugEnabled()) LOG.debug("Training data transpose size: " + trainingDataTranspose.getRowDimension() + "x" + trainingDataTranspose.getColumnDimension());
        for(int i = 0; i < trainingDataTranspose.getRowDimension(); i++){
            RealVector iRowVector = new ArrayRealVector(trainingDataTranspose.getRow(i));
            RealVector transposePC1Vect = transposePC1.getRowVector(0);
            double distance = iRowVector.getDistance(transposePC1Vect);
            if(distance > maxDistance){
                maxDistance = distance;
                p1 = iRowVector;
                p2 = transposePC1Vect;
            }
            if(distance < minDistance)
                minDistance = distance;
            //}
        }
        maximumL2Norm.setEntry(index, maxDistance);
        minimumL2Norm.setEntry(index, minDistance);
        minVector = p1;
        maxVector = p2;
        return trainingDataTranspose;
    }

    @Override
    public List<UserProfileEigenModel> generate(String site, String user, RealMatrix matrix) {
        LOG.info(String.format("Receive aggregated user activity matrix: %s size: %s x %s",user,matrix.getRowDimension(),matrix.getColumnDimension()));
        computeStats(matrix);
        RealMatrix normalizedInputMatrix = normalizeData(matrix);
        int lowVariantColumnCount = 0;
        for(int j=0; j < normalizedInputMatrix.getColumnDimension();j++){
            if(statistics[j].isLowVariant()){
                lowVariantColumnCount++;
            }
        }

        if(normalizedInputMatrix.getColumnDimension() == lowVariantColumnCount){
            LOG.info("found user: " + user + " with all features being low variant. Nothing to do...");
            UserProfileEigenModel noopModel = new UserProfileEigenModel(System.currentTimeMillis(),site,user,null,null,0,null,null,null,null,null,statistics);
            return Arrays.asList(noopModel);
        }
        else {
            computeCovarianceAndSVD(normalizedInputMatrix, lowVariantColumnCount);
            computeDimensionWithMaxVariance();
            computePrincipalComponents();
            maximumL2Norm = new ArrayRealVector(principalComponents.length);
            minimumL2Norm = new ArrayRealVector(principalComponents.length);

            for (int i = 0; i < principalComponents.length; i++) {
                RealMatrix trainingDataTranspose = computeMaxDistanceOnPCs(i);
            }

            UserProfileEigenModel userprofileEigenModel = new UserProfileEigenModel(System.currentTimeMillis(),site,user, uMatrix, diagonalMatrix, dimension, minVector, maxVector, principalComponents, maximumL2Norm, minimumL2Norm, statistics);
            return Arrays.asList(userprofileEigenModel);
        }
    }

    @Override
    public UserProfileContext context() {
        return new UserProfileContext(UserProfileConstants.EIGEN_DECOMPOSITION_ALGORITHM);
    }
}