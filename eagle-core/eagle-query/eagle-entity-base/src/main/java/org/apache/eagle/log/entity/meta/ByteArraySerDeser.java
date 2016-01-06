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

package org.apache.eagle.log.entity.meta;

/**
 * serialize byte array which is stored like the following
 */
public class ByteArraySerDeser implements EntitySerDeser<byte[]>{
    @Override
    public byte[] deserialize(byte[] bytes) {
        byte[] ret = new byte[bytes.length];
        System.arraycopy(bytes, 0, ret, 0, bytes.length);
        return ret;
    }

    @Override
    public byte[] serialize(byte[] bytes) {
        byte[] ret = new byte[bytes.length];
        System.arraycopy(bytes, 0, ret, 0, bytes.length);
        return ret;
    }

    @Override
    public Class<byte[]> type() {
        return byte[].class;
    }
}
