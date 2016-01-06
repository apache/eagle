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

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Map;

/**
 * delta event key serializer
 */
public class DeltaEventValueDeserializer implements Deserializer<DeltaEventValue> {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaEventValueDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public DeltaEventValue deserialize(String topic, byte[] data) {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInput in = null;
        DeltaEventValue ret = null;
        try{
            in = new ObjectInputStream(bis);
            ret = (DeltaEventValue)in.readObject();
        }catch(Exception ex){
            LOG.error("error deserializing object, ignore this event", ex);
            throw new RuntimeException(ex);
        }finally{
            try {
                bis.close();
                in.close();
            }catch(Exception ex){}
        }
        return ret;
    }

    @Override
    public void close() {
    }
}
