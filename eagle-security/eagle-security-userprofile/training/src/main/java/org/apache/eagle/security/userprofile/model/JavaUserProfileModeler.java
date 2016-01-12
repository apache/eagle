/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.eagle.security.userprofile.model;

import org.apache.commons.math3.linear.RealMatrix;
import scala.collection.JavaConversions;
import scala.collection.immutable.List;

/**
 * @since 10/21/15
 */
public abstract class JavaUserProfileModeler<M,C extends UserProfileContext> implements UserProfileModeler<M,C> {
    @Override
    public List<M> build(String site, String user, RealMatrix matrix) {
        java.util.List<M> models = generate(site, user, matrix);
        if (models != null) {
            return JavaConversions.asScalaIterable(models).toList();
        } else {
            return null;
        }
    }

    /**
     * Java delegate method for {@code eagle.security.userprofile.model.JavaUserProfileModeler#build(String site, String user, RealMatrix matrix)}
     *
     * @param site eagle site
     * @param user eagle user
     * @param matrix user activity matrix
     * @return Generate user profile model
     */
    public abstract java.util.List<M> generate(String site, String user, RealMatrix matrix);
}