package org.apache.eagle.security.userprofile;

import org.apache.eagle.ml.model.MLCallbackResult;
import org.apache.eagle.security.userprofile.impl.UserProfileAnomalyEigenEvaluator;
import org.apache.eagle.security.userprofile.model.UserActivityAggModel;
import org.apache.eagle.security.userprofile.model.UserCommandStatistics;
import org.apache.eagle.security.userprofile.model.UserProfileEigenModel;
import junit.framework.Assert;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.eagle.security.userprofile.impl.UserProfileAnomalyEigenEvaluator;
import org.apache.eagle.security.userprofile.model.UserProfileEigenModel;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestUserProfileAnomalyEigenEvaluator {

    private static Logger LOG = LoggerFactory.getLogger(TestUserProfileAnomalyEigenEvaluator.class);

    @Test
    public void testDetect(){
        UserProfileAnomalyEigenEvaluator eigenEvaluator = new UserProfileAnomalyEigenEvaluator();
        String[] testCmdType = {"getfileinfo", "open", "listStatus", "setTimes", "setPermission", "rename",
                                    "mkdirs", "create", "setReplication", "contentSummary", "delete", "setOwner",
                                    "fsck"};
        List<String> tmpCmdTypesAsList = new ArrayList<String>();
        tmpCmdTypesAsList = Arrays.asList(testCmdType);
        Seq<String> testCmd = JavaConversions.asScalaBuffer(tmpCmdTypesAsList);
        String testSite = "sandbox";
        long testTimestamp = 14054440;
        String testUser = "test_user";
        RealMatrix testMatrix = new Array2DRowRealMatrix(1,testCmdType.length);
        for(int i=0; i < testMatrix.getColumnDimension();i++)
            testMatrix.addToEntry(0, i, 3.0);

        UserActivityAggModel testAggModel = new UserActivityAggModel(testUser, testMatrix, testCmd,testSite, testTimestamp);

        Long testVersion = new Long(1);
        RealMatrix testUMat = new Array2DRowRealMatrix(testCmdType.length,testCmdType.length);
        RealMatrix testDiagonalMat = new Array2DRowRealMatrix(testCmdType.length, testCmdType.length);

        for(int i=0; i< testCmdType.length; i++){
            for(int  j=0; j < testCmdType.length; j++){
                testUMat.addToEntry(i,j,1.0);
                testDiagonalMat.addToEntry(i,j,1.0);
            }
        }

        int dimension = testCmdType.length -1;
        double[] minVector = new double[testCmdType.length];
        double[] maxVector = new double[testCmdType.length];
        for(int i=0; i < minVector.length;i++) {
            minVector[i] = 1;
            maxVector[i] = 1;
        }

        RealVector testMinVec = new ArrayRealVector(minVector);
        RealVector testMaxVec = new ArrayRealVector(maxVector);
        RealVector[] testPCs = new ArrayRealVector[testCmdType.length];

        for(int i =0; i < testCmdType.length; i++) {
            testPCs[i] = new ArrayRealVector(testCmdType.length);
            for(int j = 0; j < testPCs[i].getDimension(); j++){
                testPCs[i].addToEntry(j, 1.0);
            }
        }

        RealVector testMaxL2Norm = new ArrayRealVector(maxVector);
        RealVector testMinL2Norm = new ArrayRealVector(minVector);
        UserCommandStatistics userCommandStatistics[] = new UserCommandStatistics[testCmdType.length];

        for(int i=0; i< testCmdType.length;i++){
            userCommandStatistics[i] = new UserCommandStatistics();
            userCommandStatistics[i].setCommandName(testCmdType[i]);
            userCommandStatistics[i].setLowVariant(false);
            userCommandStatistics[i].setMean(1.0);
            userCommandStatistics[i].setStddev(1.0);
        }

        UserProfileEigenModel testEigenModel = new UserProfileEigenModel(testVersion,testSite,testUser,testUMat,testDiagonalMat,dimension, testMinVec, testMaxVec, testPCs, testMaxL2Norm, testMinL2Norm, userCommandStatistics);

        List<MLCallbackResult> testResults = eigenEvaluator.detect("test_user", "eigen", testAggModel, testEigenModel);

        Assert.assertEquals(testResults.size(), testMatrix.getRowDimension());
        for(MLCallbackResult result: testResults){
            Assert.assertEquals(result.isAnomaly(), true);
        }
    }
}
