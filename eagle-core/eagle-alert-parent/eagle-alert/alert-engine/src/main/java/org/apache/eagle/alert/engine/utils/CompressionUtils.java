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
package org.apache.eagle.alert.engine.utils;

import com.google.common.io.ByteStreams;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class CompressionUtils {
    public static byte[] compress(byte[] source) throws IOException {
        if (source == null || source.length == 0) {
            return source;
        }
        ByteArrayInputStream sourceStream = new ByteArrayInputStream(source);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(source.length / 2);
        try (OutputStream compressor = new GZIPOutputStream(outputStream)) {
            ByteStreams.copy(sourceStream, compressor);
            compressor.close();
        }
        try {
            return outputStream.toByteArray();
        } finally {
            sourceStream.close();
            outputStream.close();
        }
    }

    public static byte[] decompress(byte[] compressed) throws IOException{
        if (compressed == null || compressed.length == 0) {
            return compressed;
        }
        ByteArrayInputStream sourceStream = new ByteArrayInputStream(compressed);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(compressed.length * 2);
        try (GZIPInputStream compressor = new GZIPInputStream(sourceStream)) {
            ByteStreams.copy(compressor, outputStream);
            compressor.close();
        }
        try {
            return outputStream.toByteArray();
        } finally {
            sourceStream.close();
            outputStream.close();
        }
    }
}