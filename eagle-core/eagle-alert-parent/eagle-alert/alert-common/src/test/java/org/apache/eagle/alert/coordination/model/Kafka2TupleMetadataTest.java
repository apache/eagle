/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.alert.coordination.model;

import org.junit.Assert;
import org.junit.Test;

public class Kafka2TupleMetadataTest {
    @Test
    public void testKafka2TupleMetadata() {
        Kafka2TupleMetadata kafka2TupleMetadata = new Kafka2TupleMetadata();
        kafka2TupleMetadata.setName("setName");
        kafka2TupleMetadata.setCodec(new Tuple2StreamMetadata());
        kafka2TupleMetadata.setType("setType");
        kafka2TupleMetadata.setTopic("setTopic");
        kafka2TupleMetadata.setSchemeCls("org.apache.eagle.alert.engine.scheme.PlainStringScheme");

        Kafka2TupleMetadata kafka2TupleMetadata1 = new Kafka2TupleMetadata();
        kafka2TupleMetadata1.setName("setName");
        kafka2TupleMetadata1.setCodec(new Tuple2StreamMetadata());
        kafka2TupleMetadata1.setType("setType");
        kafka2TupleMetadata1.setTopic("setTopic");
        kafka2TupleMetadata1.setSchemeCls("org.apache.eagle.alert.engine.scheme.PlainStringScheme");

        Assert.assertFalse(kafka2TupleMetadata1 == kafka2TupleMetadata);
        Assert.assertTrue(kafka2TupleMetadata1.equals(kafka2TupleMetadata));
        Assert.assertTrue(kafka2TupleMetadata1.hashCode() == kafka2TupleMetadata.hashCode());

        kafka2TupleMetadata1.setType("setType1");

        Assert.assertFalse(kafka2TupleMetadata1.equals(kafka2TupleMetadata));
        Assert.assertFalse(kafka2TupleMetadata1.hashCode() == kafka2TupleMetadata.hashCode());
    }
}
