/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.spout;

import java.util.Map;

import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;

/**
 * topic to stream metadata lifecycle method
 * one topic may spawn multiple streams, the metadata change includes
 * 1. add/remove stream
 * 2. for a specific stream, groupingstrategy is changed
 * ex1, this stream has more alert bolts than before, then this spout would take more traffic
 * ex2, this stream has less alert bolts than before, then this spout would take less traffic
 */
public interface ISpoutSpecLCM {
    /**
     * stream metadata is used for SPOUT to filter traffic and route traffic to following groupby bolts.
     *
     * @param metadata
     */
    void update(SpoutSpec metadata, Map<String, StreamDefinition> sds);
}