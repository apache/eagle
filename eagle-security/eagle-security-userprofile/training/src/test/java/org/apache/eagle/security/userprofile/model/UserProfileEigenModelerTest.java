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

import junit.framework.Assert;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.eagle.security.userprofile.model.eigen.UserProfileEigenModeler;
import scala.collection.immutable.List;

public class UserProfileEigenModelerTest {

    @org.junit.Test
    public void testBuild() throws Exception {

        UserProfileEigenModeler modeler = new UserProfileEigenModeler();
        String user = "user1";
        final RealMatrix mockMatrix = new Array2DRowRealMatrix(buildMockData());
        List<UserProfileEigenModel> model = modeler.build("default",user, mockMatrix);
        Assert.assertEquals(model.length(), 1);

        UserProfileEigenModel eigenModel = model.head();
        Assert.assertNotNull(eigenModel.statistics());
        Assert.assertNotNull(eigenModel.principalComponents());
        Assert.assertNotNull(eigenModel.maxVector());
        Assert.assertNotNull(eigenModel.minVector());
        Assert.assertEquals(eigenModel.statistics().length, mockMatrix.getColumnDimension());
        Assert.assertTrue(eigenModel.principalComponents().length <= mockMatrix.getColumnDimension());
        Assert.assertTrue(eigenModel.maxVector().getDimension() <= mockMatrix.getColumnDimension());
        Assert.assertTrue(eigenModel.minVector().getDimension() <= mockMatrix.getColumnDimension());
        Assert.assertEquals(true, eigenModel.statistics()[3].isLowVariant());
    }

    private double[][] buildMockData(){
        double[][] mockData = new double[][]{
                {114503.0,2.8820906E7,123618.0,0.0,64.0,15772.0,186.0,24296.0,12.0,9.0,32.0,0.0,0.0},
                {53300.0,390772.0,157626.0,0.0,67.0,10501.0,226.0,8567.0,14.0,0.0,12.0,0.0,0.0},
                {25659.0,140858.0,35731.0,0.0,169.0,1619.0,520.0,2965.0,34.0,0.0,34.0,0.0,0.0}
        };
        return mockData;
    }
}