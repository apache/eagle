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

package org.apache.eagle.state.deltaevent;

import java.io.Closeable;

/**
 * interface for persisting delta events which can be used for replaying events based on latest snapshot while doing failover
 */
public interface DeltaEventDAO extends Closeable{
    /**
     * writeState event and return id which represents that event,
     * the return id will be used for replaying
     * @return
     * @throws Exception
     */
    long write(Object event) throws Exception;

    void load(long startOffset, DeltaEventReplayCallback callback) throws Exception;
}
