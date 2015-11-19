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
package org.apache.eagle.security.userprofile.model.kde;

import org.apache.eagle.security.userprofile.UserProfileConstants;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.eagle.security.userprofile.model.JavaUserProfileModeler;
import org.apache.eagle.security.userprofile.model.UserCommandStatistics;
import org.apache.eagle.security.userprofile.model.UserProfileContext;
import org.apache.eagle.security.userprofile.model.UserProfileKDEModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class UserProfileKDEModeler extends JavaUserProfileModeler<UserProfileKDEModel,UserProfileContext> {

    private final String[] cmdTypes;
    private UserCommandStatistics[] statistics;
    private RealMatrix finalMatrixWithoutLowVariantCmds;
    private RealMatrix covarianceMatrix;
    private double[] probabilityEstimation;
    private double minProbabilityEstimate;
    private double maxProbabilityEstimate;
    private double nintyFivePercentileEstimate;
    private double medianProbabilityEstimate;
    private final static Logger LOG = LoggerFactory.getLogger(UserProfileKDEModeler.class);
    private static final double lowVarianceVal= 0.001;

    public UserProfileKDEModeler(String[] cmdTypes){
        this.cmdTypes = cmdTypes;
    }

    public UserProfileKDEModeler(){
        this.cmdTypes = UserProfileConstants.DEFAULT_CMD_TYPES;
    }

    private void computeStats(RealMatrix m){
        if(m.getColumnDimension() !=  this.cmdTypes.length){
            LOG.error("Please fix the commands list in config file");
        }

        statistics = new UserCommandStatistics[m.getColumnDimension()];

        for(int i=0; i<m.getColumnDimension(); i++){
            UserCommandStatistics stats = new UserCommandStatistics();
            stats.setCommandName(this.cmdTypes[i]);
            RealVector colData = m.getColumnVector(i);
            StandardDeviation deviation = new StandardDeviation();
            double stddev = deviation.evaluate(colData.toArray());

            if(LOG.isDebugEnabled()) LOG.debug("Stddev is NAN ? " + (Double.isNaN(stddev) ? "yes" : "no"));
            if(stddev <= lowVarianceVal)
                stats.setLowVariant(true);
            else
                stats.setLowVariant(false);

            stats.setStddev(stddev);
            Mean mean = new Mean();
            double mu = mean.evaluate(colData.toArray());
            if(LOG.isDebugEnabled()) LOG.debug("mu is NAN ? " + (Double.isNaN(mu)? "yes":"no"));

            stats.setMean(mu);
            statistics[i]=stats;
        }
    }

    private void computeProbabilityDensityEstimation(RealMatrix inputMat){

        probabilityEstimation = new double[inputMat.getRowDimension()];
        for(int i=0; i < probabilityEstimation.length; i++)
            probabilityEstimation[i] = 1.0;

        for(int i=0; i < inputMat.getRowDimension(); i++){
            for(int j=0; j < inputMat.getColumnDimension(); j++){
                if(statistics[j].getStddev() > 0){
                    double stddev = statistics[j].getStddev();
                    double mean = statistics[j].getMean();
                    double sqrt2PI = Math.sqrt(2.0*Math.PI);
                    double denominatorFirstPart = sqrt2PI*stddev;
                    double squareMeanNormal = Math.pow((inputMat.getEntry(i, j) - mean), 2);
                    double twoPowStandardDev = Math.pow(stddev, 2);
                    double twoTimesTwoPowStandardDev = 2.0*twoPowStandardDev;
                    probabilityEstimation[i] *= ((1.00/denominatorFirstPart)
                            *(Math.exp(-(squareMeanNormal/twoTimesTwoPowStandardDev))));
                }
            }
        }

        java.util.List<Double> listProb = new ArrayList<Double>();
        for(int i=0; i < probabilityEstimation.length; i++){
            probabilityEstimation[i] = Math.log10(probabilityEstimation[i]);
            listProb.add(probabilityEstimation[i]);
        }

        Collections.sort(listProb);
        int i=0;
        for(double d:listProb)
            probabilityEstimation[i++]=d;

        minProbabilityEstimate = probabilityEstimation[probabilityEstimation.length -1];
        maxProbabilityEstimate = probabilityEstimation[0];

        int len = probabilityEstimation.length;
        int nintyFivePercentIndex = (int)Math.round(0.05*len);
        int medianPercentIndex = (int)Math.round(0.5*len);
        if(medianPercentIndex >= len)
            medianProbabilityEstimate = probabilityEstimation[medianPercentIndex-1];
        else
            medianProbabilityEstimate = probabilityEstimation[medianPercentIndex];
        nintyFivePercentileEstimate = probabilityEstimation[nintyFivePercentIndex];
    }

    @Override
    public List<UserProfileKDEModel> generate(String site, String user, RealMatrix matrix) {
        LOG.info(String.format("Receive aggregated user activity matrix: %s size: %s x %s",user,matrix.getRowDimension(),matrix.getColumnDimension()));
        computeStats(matrix);
        computeProbabilityDensityEstimation(matrix);
        UserProfileKDEModel userprofileKDEModel = new UserProfileKDEModel(System.currentTimeMillis(),site,user, statistics, minProbabilityEstimate, maxProbabilityEstimate, nintyFivePercentileEstimate, medianProbabilityEstimate);
        return Arrays.asList(userprofileKDEModel);
    }

    @Override
    public UserProfileContext context() {
        return new UserProfileContext(UserProfileConstants.KDE_ALGORITHM);
    }
}