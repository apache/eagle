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
package org.apache.eagle.alert.engine.coordinator;

import java.io.Closeable;
import java.io.Serializable;

import org.apache.eagle.alert.engine.publisher.AlertPublishSpecListener;
import org.apache.eagle.alert.engine.router.AlertBoltSpecListener;
import org.apache.eagle.alert.engine.router.SpecListener;
import org.apache.eagle.alert.engine.router.SpoutSpecListener;
import org.apache.eagle.alert.engine.router.StreamRouterBoltSpecListener;

import com.typesafe.config.Config;

/**
 * IMetadataChangeNotifyService defines the following features
 * 1) initialization
 * 2) register metadata change listener
 *
 * In distributed environment for example storm platform,
 * subclass implementing this interface should have the following lifecycle
 * 1. object is created in client machine
 * 2. object is serialized and transferred to other machine
 * 3. object is created through deserialization
 * 4. invoke init() method to do initialization
 * 5. invoke various registerListener to get notified of config change
 * 6. invoke close() to release system resource
 */
public interface IMetadataChangeNotifyService extends Closeable,Serializable {
    /**
     *
     * @param config
     */
    void init(Config config, MetadataType type);

    void registerListener(SpoutSpecListener listener);

    void registerListener(AlertBoltSpecListener listener);

    void registerListener(StreamRouterBoltSpecListener listener);

    void registerListener(AlertPublishSpecListener listener);

    void registerListener(SpecListener listener);
}