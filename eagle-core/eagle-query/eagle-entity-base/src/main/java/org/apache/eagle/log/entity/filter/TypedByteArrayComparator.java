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
package org.apache.eagle.log.entity.filter;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * <h1>TypedByteArrayComparator</h1>
 *
 * Compare byte array: <code>byte[] value</code> with class type: <code>Class type</code>
 *
 * <br/>
 * <br/>
 * Built-in support:
 *
 *  <pre>
 *    Double
 *    double
 *    Integer
 *    int
 *    Long
 *    long
 *    Short
 *    short
 *    Boolean
 *    boolean
 *  </pre>
 *
 *  And can be extend by defining new {@link RawComparator} and register with  {@link #define(Class type, RawComparator comparator)}
 * <br/>
 * <br/>
 */
public class TypedByteArrayComparator extends ByteArrayComparable {
    private final static Logger LOG = LoggerFactory.getLogger(TypedByteArrayComparator.class);

    private Class type;

    // Not need to be writable
    private RawComparator comparator;

    /**
     * Default constructor for writable
     */
    @SuppressWarnings("unused")
    public TypedByteArrayComparator(){
        super(null);
    }

    public TypedByteArrayComparator(byte[] value, Class type){
        super(value);
        this.type = type;
        this.comparator = get(this.type);
        if(this.comparator == null) throw new IllegalArgumentException("No comparator found for class: "+type);
    }

    /**
     * @param in hbase-0.94 interface
     * @throws IOException
     */
//    @Override
    public void readFields(DataInput in) throws IOException {
//        super.readFields(in);
        try {
            String _type = in.readUTF();
            type = _primitiveTypeClassMap.get(_type);
            if(type == null) {
                type = Class.forName(_type);
            }
            comparator = get(type);
            if(comparator == null) throw new IllegalArgumentException("No comparator found for class: "+type);
        } catch (ClassNotFoundException e) {
            throw new IOException(e.getMessage(),e);
        }
    }

    /**
     * @param out hbase-0.94 interface
     * @throws IOException
     */
//    @Override
    public void write(DataOutput out) throws IOException {
//        super.write(out);
        String typeName = type.getName();
        out.writeUTF(typeName);
    }

    /**
     * For hbase 0.98
     *
     * @return serialized byte array
     */
    @Override
    public byte[] toByteArray() {
        ByteArrayDataOutput byteArrayDataOutput = ByteStreams.newDataOutput();
        try {
            this.write(byteArrayDataOutput);
            return byteArrayDataOutput.toByteArray();
        } catch (IOException e) {
            LOG.error("Failed to serialize due to: "+e.getMessage(),e);
            throw new RuntimeException(e);
        }
    }

    /**
     * For hbase 0.98
     *
     * @param bytes raw byte array
     * @return Comparator instance
     * @throws DeserializationException
     */
    public static TypedByteArrayComparator parseFrom(final byte [] bytes)
            throws DeserializationException {
        TypedByteArrayComparator comparator = new TypedByteArrayComparator();
        ByteArrayDataInput byteArrayDataInput = ByteStreams.newDataInput(bytes);
        try {
            comparator.readFields(byteArrayDataInput);
        } catch (IOException e) {
            LOG.error("Got error to deserialize TypedByteArrayComparator from PB bytes",e);
            throw new DeserializationException(e);
        }
        return comparator;
    }

    @Override
    public int compareTo(byte[] value, int offset, int length) {
        return this.comparator.compare(this.getValue(), 0, this.getValue().length, value, offset, length);
    }

    /**
     * <ol>
     * <li>Try registered comparator</li>
     * <li>If not found, try all possible WritableComparator</li>
     * </ol>
     *
     * If not found finally, throw new IllegalArgumentException("unable to get comparator for class: "+type);
     *
     * @param type value type class
     * @return RawComparator
     */
    public static RawComparator get(Class type){
        RawComparator comparator = null;
        try {
            comparator = _typedClassComparator.get(type);
        }catch (ClassCastException ex){
            // ignore
        }
        try {
            if (comparator == null) comparator = WritableComparator.get(type);
        }catch (ClassCastException ex){
            // ignore
        }
        return comparator;
    }

    private final static Map<Class,RawComparator> _typedClassComparator = new HashMap<Class, RawComparator>();
    public static void define(Class type, RawComparator comparator){
        _typedClassComparator.put(type,comparator);
    }

    static{
        define(Double.class, WritableComparator.get(DoubleWritable.class));
        define(double.class, WritableComparator.get(DoubleWritable.class));
        define(Integer.class, WritableComparator.get(IntWritable.class));
        define(int.class, WritableComparator.get(IntWritable.class));
        define(Long.class, WritableComparator.get(LongWritable.class));
        define(long.class, WritableComparator.get(LongWritable.class));
        define(Short.class, WritableComparator.get(ShortWritable.class));
        define(short.class, WritableComparator.get(ShortWritable.class));
        define(Boolean.class, WritableComparator.get(BooleanWritable.class));
        define(boolean.class, WritableComparator.get(BooleanWritable.class));
    }

    /**
     * Because {@link Class#forName } can't find class for primitive type
     */
    private final static Map<String,Class> _primitiveTypeClassMap = new HashMap<String, Class>();
    static {
        _primitiveTypeClassMap.put(int.class.getName(),int.class);
        _primitiveTypeClassMap.put(double.class.getName(),double.class);
        _primitiveTypeClassMap.put(long.class.getName(),long.class);
        _primitiveTypeClassMap.put(short.class.getName(),short.class);
        _primitiveTypeClassMap.put(boolean.class.getName(),boolean.class);
        _primitiveTypeClassMap.put(char.class.getName(),char.class);
        _primitiveTypeClassMap.put(byte.class.getName(),byte.class);
    }
}