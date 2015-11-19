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
package org.apache.eagle.security.userprofile.model;

import org.apache.eagle.security.userprofile.model.kde.UserProfileKDEModeler;
import junit.framework.Assert;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.eagle.security.userprofile.model.kde.UserProfileKDEModeler;

public class UserProfileKDEModelerTest {

    @org.junit.Test
    public void testBuild() throws Exception {
        UserProfileKDEModeler modeler = new UserProfileKDEModeler();
        String user = "user1";
        final RealMatrix mockMatrix = new Array2DRowRealMatrix(buildMockData());
        scala.collection.immutable.List<UserProfileKDEModel> testStatistics = modeler.build("default",user, mockMatrix);
        Assert.assertEquals(testStatistics.length(), 1);
        UserProfileKDEModel model = testStatistics.head();
        Assert.assertEquals(model.user(), "user1");
        Assert.assertEquals(model.statistics().length, mockMatrix.getColumnDimension());
        Assert.assertEquals(-36, model.minProbabilityEstimate(), 1.0);
        Assert.assertEquals(-37, model.maxProbabilityEstimate(), 1.0);
        Assert.assertEquals(-37, model.nintyFivePercentileEstimate(), 1.0);
        Assert.assertEquals(-37, model.medianProbabilityEstimate(), 1.0);
        Assert.assertEquals(true, model.statistics()[3].isLowVariant());
        Assert.assertEquals(false, model.statistics()[0].isLowVariant());
        Assert.assertEquals("setTimes", model.statistics()[3].getCommandName());
        Assert.assertEquals("getfileinfo", model.statistics()[0].getCommandName());
    }

    private double[][] buildMockData(){
        double[][] mockData = new double[][]{
                {114503.0,2.8820906E7,123618.0,0.0,64.0,15772.0,186.0,24296.0,12.0,9.0,32.0},
                {53300.0,390772.0,157626.0,0.0,67.0,10501.0,226.0,8567.0,14.0,0.0,12.0},
                {25659.0,140858.0,35731.0,0.0,169.0,1619.0,520.0,2965.0,34.0,0.0,34.0}
        };
        return mockData;
    }
}