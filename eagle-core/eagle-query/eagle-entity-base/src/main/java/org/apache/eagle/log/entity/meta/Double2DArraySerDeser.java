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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @since 7/22/15
 */
public class Double2DArraySerDeser implements EntitySerDeser<double[][]> {
    private final int SIZE = 8;
    @Override
    public double[][] deserialize(byte[] bytes){
//        if((bytes.length-4) % SIZE != 0)
//            return null;
        int offset = 0;
        // get size of int array
        int rowSize = ByteUtil.bytesToInt(bytes, offset);
        offset += 4;

        double[][] data = new double[rowSize][];
        for(int i=0; i<rowSize; i++) {
            int colSize = ByteUtil.bytesToInt(bytes, offset);
            offset += 4;
            double[] values = null;
            if (colSize >= 0){
                values = new double[colSize];
                for (int j = 0; j < colSize; j++) {
                    values[j] = ByteUtil.bytesToDouble(bytes, offset);
                    offset += SIZE;
                }
            }
            data[i] = values;
        }

        return data;
    }

    /**
     *
     * @param obj
     * @return
     */
    @Override
    public byte[] serialize(double[][] obj){
        if(obj == null) return null;
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        int size = obj.length;
        byte[] sizeBytes = ByteUtil.intToBytes(size);
        data.write(sizeBytes,0,sizeBytes.length);

        try{
            for(double[] o:obj){
                if(o!=null){
                    data.write(ByteUtil.intToBytes(o.length));
                    for(double d:o){
                        data.write(ByteUtil.doubleToBytes(d),0,SIZE);
                    }
                }else{
                    data.write(ByteUtil.intToBytes(-1),0,4);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[] bytes = data.toByteArray();
        try {
            data.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return bytes;
    }

    @Override
    public Class<double[][]> type() {
        return double[][].class;
    }
}