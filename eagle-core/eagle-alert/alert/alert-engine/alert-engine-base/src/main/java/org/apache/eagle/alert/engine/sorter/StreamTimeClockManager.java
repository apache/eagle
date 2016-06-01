/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.sorter;

import java.io.Serializable;

/**
 *
 * By default, we could keep the current time clock in memory,
 * Eventually we may need to consider the global time synchronization across all nodes
 *
 * TODO: maybe need to synchronize time clock globally
 *
 * 1) When to initialize window according to start time
 * 2) When to close expired window according to current time
 * 3) Automatically tick periodically as the single place for control lock
 *
 */
public interface StreamTimeClockManager extends StreamTimeClockTrigger, Serializable{
    /**
     * @return StreamTimeClock instance
     */
    StreamTimeClock createStreamTimeClock(String streamId);

    StreamTimeClock getStreamTimeClock(String streamId);

    /**
     * @param streamId
     */
    void removeStreamTimeClock(String streamId);
}