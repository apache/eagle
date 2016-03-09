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
package org.apache.eagle.ml;

import org.apache.eagle.dataproc.core.ValuesArray;
import org.apache.eagle.ml.model.MLAlgorithm;
import com.typesafe.config.Config;

/**
 * Machine Learning Algorithm Evaluator
 */
public interface MLAlgorithmEvaluator {
    /**
     * Prepare Machine learning algorithm
     *
     * @param algorithm MLAlgorithm instance
     */
    public void init(MLAlgorithm algorithm, Config config);

    /**
     * Evaluate input user profile model
     *
     * @param data ValuesArray
     * @throws Exception
     */
	public void evaluate(ValuesArray data) throws Exception;

    /**
     * Register callback
     *
     * @param callbackObj MachineLearningCallback
     * @throws Exception
     */
	public void register(MLAnomalyCallback callbackObj) throws Exception;
}