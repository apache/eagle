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

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.ml.model.MLModelAPIEntity;
import org.apache.eagle.security.userprofile.UserProfileConstants;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.eagle.security.userprofile.UserProfileConstants;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserProfileEigenModelEntity {
    private final static Logger LOG = LoggerFactory.getLogger(UserProfileEigenModelEntity.class);

    public static MLModelAPIEntity serializeModel(UserProfileEigenModel model) throws IOException {
        if(model == null) return null;
        if(model.user()==null) throw new IllegalArgumentException("user is null");

        MLModelAPIEntity entity = new MLModelAPIEntity();
        UserProfileEigenModelEntity content = new UserProfileEigenModelEntity();
        entity.setVersion(model.version());
        if(model.uMatrix()!=null) content.setUMatrix(model.uMatrix().getData());
        if(model.diagonalMatrix()!=null) content.setDiagonalMatrix(model.diagonalMatrix().getData());
        content.setDimension(model.dimension());
        if(model.minVector()!=null) content.setMinVector(model.minVector().toArray());
        if(model.maxVector()!=null) content.setMaxVector(model.maxVector().toArray());

        RealVector[] pcsInVector = model.principalComponents();
        if(pcsInVector!=null) {
            List<double[]> prcsInDouleArrayList = new ArrayList<>(pcsInVector.length);
            for (RealVector vector : pcsInVector) prcsInDouleArrayList.add(vector.toArray());
            content.setPrincipalComponents(prcsInDouleArrayList);
        }
        if(model.maximumL2Norm()!=null) content.setMaximumL2Norm(model.maximumL2Norm().toArray());
        if(model.minimumL2Norm()!=null) content.setMinimumL2Norm(model.minimumL2Norm().toArray());
        if(model.statistics()!=null){
            List<Map<String,Object>> statisticsMaps = new ArrayList<>(model.statistics().length);
            for(UserCommandStatistics userCommandStatistics :model.statistics()) statisticsMaps.add(userCommandStatistics.toMap());
            content.setStatistics(statisticsMaps);
        }

        entity.setContent(TaggedLogAPIEntity.buildObjectMapper().writeValueAsString(content));
        Map<String,String> tags = new HashMap<>();
        tags.put(UserProfileConstants.SITE_TAG,model.site());
        tags.put(UserProfileConstants.USER_TAG,model.user());
        tags.put(UserProfileConstants.ALGORITHM_TAG,model.algorithm());

        entity.setTags(tags);
        return entity;
    }

    public  static UserProfileEigenModel deserializeModel(MLModelAPIEntity entity) throws IOException {
        if(entity == null) return null;
        String user = entity.getTags() == null ? null: entity.getTags().get(UserProfileConstants.USER_TAG);
        String site = entity.getTags()==null? null:entity.getTags().get(UserProfileConstants.SITE_TAG);

        UserProfileEigenModelEntity content = TaggedLogAPIEntity.buildObjectMapper().readValue(entity.getContent(), UserProfileEigenModelEntity.class);
        RealVector[] principalComponents = null;
        if(content.getPrincipalComponents()!=null){
            principalComponents = new RealVector[content.getPrincipalComponents().size()];
            int i = 0;
            for(double[] array:content.getPrincipalComponents()){
                principalComponents[i] = new ArrayRealVector(array);
                i++;
            }
        }
        UserCommandStatistics[] statistics = null;
        if(content.getStatistics()!=null){
            statistics = new UserCommandStatistics[content.getStatistics().size()];
            int i = 0;
            for(Map<String,Object> map:content.getStatistics()){
                statistics[i++] = new UserCommandStatistics(map);
            }
        }

        return new UserProfileEigenModel(
                entity.getVersion(),
                site,
                user,
                content.getUMatrix() == null? null : new Array2DRowRealMatrix(content.getUMatrix()),
                content.getDiagonalMatrix() == null ? null : new Array2DRowRealMatrix(content.getDiagonalMatrix()),
                content.getDimension(),
                content.getMinVector() == null ? null : new ArrayRealVector(content.getMinVector()),
                content.getMaxVector() == null? null : new ArrayRealVector(content.getMaxVector()),
                principalComponents,
                content.getMaximumL2Norm() == null ? null : new ArrayRealVector(content.getMaximumL2Norm()),
                content.getMinimumL2Norm() == null ? null : new ArrayRealVector(content.getMinimumL2Norm()),
                statistics
        );
    }

    private double[][] uMatrix;
    private double[][] diagonalMatrix;
    private int dimension;
    private double[] minVector;
    private double[] maxVector;
    private List<double[]> principalComponents;
    private double[] maximumL2Norm;
    private double[] minimumL2Norm;
    private List<Map<String,Object>> statistics;

    public List<Map<String, Object>> getStatistics() {
        return statistics;
    }

    public void setStatistics(List<Map<String, Object>> statistics) {
        this.statistics = statistics;
//        valueChanged("statistics");
    }

    public double[][] getUMatrix() {
        return uMatrix;
    }

    public void setUMatrix(double[][] uMatrix) {
        this.uMatrix = uMatrix;
//        valueChanged("uMatrix");
    }

    public double[][] getDiagonalMatrix() {
        return diagonalMatrix;
    }

    public void setDiagonalMatrix(double[][] diagonalMatrix) {
        this.diagonalMatrix = diagonalMatrix;
//        valueChanged("diagonalMatrix");
    }

    public int getDimension() {
        return dimension;
    }

    public void setDimension(int dimension) {
        this.dimension = dimension;
//        valueChanged("dimension");
    }

    public double[] getMinVector() {
        return minVector;
    }

    public void setMinVector(double[] minVector) {
        this.minVector = minVector;
    }

    public double[] getMaxVector() {
        return maxVector;
    }

    public void setMaxVector(double[] maxVector) {
        this.maxVector = maxVector;
    }

    public List<double[]> getPrincipalComponents() {
        return principalComponents;
    }

    public void setPrincipalComponents(List<double[]> principalComponents) {
        this.principalComponents = principalComponents;
    }

    public double[] getMaximumL2Norm() {
        return maximumL2Norm;
    }

    public void setMaximumL2Norm(double[] maximumL2Norm) {
        this.maximumL2Norm = maximumL2Norm;
    }

    public double[] getMinimumL2Norm() {
        return minimumL2Norm;
    }

    public void setMinimumL2Norm(double[] minimumL2Norm) {
        this.minimumL2Norm = minimumL2Norm;
    }
}