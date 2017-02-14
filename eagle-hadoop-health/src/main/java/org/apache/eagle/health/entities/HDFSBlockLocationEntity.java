/*
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

package org.apache.eagle.health.entities;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.apache.hadoop.fs.BlockLocation;

/**
 * HDFS Block Location Entity
 *
 * @see org.apache.hadoop.fs.BlockLocation
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("eagle_hdfs_blocks")
@ColumnFamily("f")
@Prefix("hdfs_blocks_location")
@Service(HDFSBlockLocationEntity.HDFS_BLOCK_LOCATION_SERVICE_NAME)
@TimeSeries(true)
@Partition({"site"})
@Tags({"site","blockId","path"})
public class HDFSBlockLocationEntity extends TaggedLogAPIEntity {
    public static final String HDFS_BLOCK_LOCATION_SERVICE_NAME = "HDFSBlockLocationService";
    @Column("a")
    private BlockLocation blockLocation;
    @Column("b")
    private long modifiedTime;
    @Column("c")
    private long createdTime;

    public BlockLocation getBlockLocation() {
        return blockLocation;
    }

    public void setBlockLocation(BlockLocation blockLocation) {
        this.blockLocation = blockLocation;
        valueChanged("blockLocation");
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(long modifiedTime) {
        this.modifiedTime = modifiedTime;
        valueChanged("modifiedTime");
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
        valueChanged("createdTime");
    }
}