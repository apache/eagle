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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * TODO: Critical performance problem!!!
 * TODO: Refactor to specified multi-column filter so that avoid return all qualifier columns from region server to client side
 *
 * @since 2014/11/17
 */
public class RowValueFilter extends FilterBase {
    private final static Logger LOG = LoggerFactory.getLogger(RowValueFilter.class);
    private boolean filterOutRow = false;
    private WritableComparable<List<KeyValue>> comparator;

    // TODO: Use qualifiers to reduce network tranfer
//    private List<byte[]> qualifiers;
    public RowValueFilter(){}

    /**
     * Filter out row if WritableComparable.compareTo return 0
     * @param comparator <code>WritableComparable[List[KeyValue]]</code>
     */
    public RowValueFilter(WritableComparable<List<KeyValue>> comparator){
        this.comparator = comparator;
    }

//    public RowValueFilter(List<byte[]> qualifiers,WritableComparable<List<KeyValue>> comparator){
//        this.qualifiers = qualifiers;
//        this.comparator = comparator;
//    }

    /**
     * Old interface in hbase-0.94
     *
     * @param out
     * @throws IOException
     */
    @Deprecated
    public void write(DataOutput out) throws IOException {
        this.comparator.write(out);
    }

    /**
     * Old interface in hbase-0.94
     *
     * @param in
     * @throws IOException
     */
//    @Override
    @Deprecated
    public void readFields(DataInput in) throws IOException {
        this.comparator = new BooleanExpressionComparator();
        this.comparator.readFields(in);
    }

    /**
     * TODO: Currently still use older serialization method from hbase-0.94, need to migrate into ProtoBuff based
     *
     * @return
     * @throws IOException
     */
    @Override
    public byte[] toByteArray() throws IOException {
        ByteArrayDataOutput byteArrayDataOutput = ByteStreams.newDataOutput();
        this.comparator.write(byteArrayDataOutput);
        return byteArrayDataOutput.toByteArray();
    }

    /**
     * TODO: Currently still use older serialization method from hbase-0.94, need to migrate into ProtoBuff based
     */
    // Override static method
    public static Filter parseFrom(final byte [] pbBytes) throws DeserializationException {
        ByteArrayDataInput byteArrayDataInput = ByteStreams.newDataInput(pbBytes);
        RowValueFilter filter = new RowValueFilter();
        try {
            filter.readFields(byteArrayDataInput);
        } catch (IOException e) {
            LOG.error("Got error to deserialize RowValueFilter from PB bytes",e);
            throw new DeserializationException(e);
        }
        return filter;
    }

    @Override
    public boolean hasFilterRow(){
        return true;
    }

    @Override
    public void filterRow(List<KeyValue> row) {
        filterOutRow = (this.comparator.compareTo(row) == 0);
    }

    @Override
    public void reset() {
        this.filterOutRow = false;
    }

    @Override
    public boolean filterRow(){
        return filterOutRow;
    }

    @Override
    public String toString() {
        return super.toString()+" ( "+this.comparator.toString()+" )";
    }

//    public List<byte[]> getQualifiers() {
//        return qualifiers;
//    }
}