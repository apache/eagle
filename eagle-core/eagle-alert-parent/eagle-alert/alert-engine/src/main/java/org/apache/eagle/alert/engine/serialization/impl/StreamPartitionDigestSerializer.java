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
package org.apache.eagle.alert.engine.serialization.impl;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.serialization.Serializer;

/**
 * Don't serialize streamId
 *
 * @see StreamPartition
 */
public class StreamPartitionDigestSerializer implements Serializer<StreamPartition> {
    public final static StreamPartitionDigestSerializer INSTANCE = new StreamPartitionDigestSerializer();

    private final Map<DigestBytes, StreamPartition> checkSumPartitionMap = new HashMap<>();
    private final Map<StreamPartition, DigestBytes> partitionCheckSumMap = new HashMap<>();

    @Override
    public void serialize(StreamPartition partition, DataOutput dataOutput) throws IOException {
        DigestBytes checkSum = partitionCheckSumMap.get(partition);
        if (checkSum == null) {
            try {
                checkSum = digestCheckSum(partition);
                partitionCheckSumMap.put(partition, checkSum);
                checkSumPartitionMap.put(checkSum, partition);
            } catch (NoSuchAlgorithmException e) {
                throw new IOException(e);
            }
        }
        dataOutput.writeInt(checkSum.size());
        dataOutput.write(checkSum.toByteArray());
    }

    @Override
    public StreamPartition deserialize(DataInput dataInput) throws IOException {
        int checkSumLen = dataInput.readInt();
        byte[] checksum = new byte[checkSumLen];
        dataInput.readFully(checksum);
        StreamPartition partition = checkSumPartitionMap.get(new DigestBytes(checksum));
        if (partition == null) {
            throw new IOException("Illegal partition checksum: " + checksum);
        }
        return partition;
    }

    private class DigestBytes {
        private final byte[] data;

        public DigestBytes(byte[] bytes) {
            this.data = bytes;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof DigestBytes && Arrays.equals(data, ((DigestBytes) other).data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }

        public int size() {
            return data.length;
        }

        public byte[] toByteArray() {
            return data;
        }
    }

    private DigestBytes digestCheckSum(Object obj) throws IOException, NoSuchAlgorithmException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        MessageDigest m = MessageDigest.getInstance("SHA1");
        m.update(baos.toByteArray());
        return new DigestBytes(m.digest());
    }
}