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
package org.apache.eagle.storage.hbase.query.coprocessor;

import org.apache.eagle.storage.hbase.query.coprocessor.generated.AggregateProtos;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

/**
 * <h1>AggregateResultCallback Interface</h1>
 *
 * Merge coprocessor results from different regions and generate final aggregate result
 * <br/>
 *
 * @see org.apache.hadoop.hbase.client.HTableInterface
 * 		coprocessorExec(Class<T> protocol, byte[] startKey, byte[] endKey, Batch.Call<T,R> callable) throws IOException, Throwable;
 *
 */
public interface AggregateResultCallback extends Batch.Callback<AggregateProtos.AggregateResult>{
	/**
	 * Generate final result after callback from region servers
	 *
	 * @return AggregateResult
	 */
    AggregateResult result();

    /**
     * Compatible for older callback interface in 0.94 or older
     *
     * @param region
     * @param row
     * @param result
     */
    void update(byte[] region, byte[] row, AggregateResult result);
}