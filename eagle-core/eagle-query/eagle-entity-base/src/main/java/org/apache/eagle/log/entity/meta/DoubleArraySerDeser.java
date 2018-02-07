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
package org.apache.eagle.log.entity.meta;

import org.apache.eagle.common.ByteUtil;

public class DoubleArraySerDeser implements EntitySerDeser<double[]> {

    private static final int SIZE = 8;

    public DoubleArraySerDeser() {
    }

    @Override
    public double[] deserialize(byte[] bytes) {
        if ((bytes.length - 4) % SIZE != 0) {
            return null;
        }
        int offset = 0;
        // get size of int array
        int size = ByteUtil.bytesToInt(bytes, offset);
        offset += 4;
        double[] values = new double[size];
        for (int i = 0; i < size; i++) {
            values[i] = ByteUtil.bytesToDouble(bytes, offset);
            offset += SIZE;
        }
        return values;
    }

    /**
     * @param obj
     * @return
     */
    @Override
    public byte[] serialize(double[] obj) {
        if (obj == null) {
            return null;
        }
        int size = obj.length;
        byte[] array = new byte[4 + SIZE * size];
        byte[] first = ByteUtil.intToBytes(size);
        int offset = 0;
        System.arraycopy(first, 0, array, offset, first.length);
        offset += first.length;
        for (int i = 0; i < size; i++) {
            System.arraycopy(ByteUtil.doubleToBytes(obj[i]), 0, array, offset, SIZE);
            offset += SIZE;
        }
        return array;
    }

    @Override
    public Class<double[]> type() {
        return double[].class;
    }
}
