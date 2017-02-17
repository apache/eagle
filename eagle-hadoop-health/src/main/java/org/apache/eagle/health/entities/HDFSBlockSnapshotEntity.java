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
 * tags {
 *   blockpool: BP-1124468226-10.0.2.15-1429879726015
 *   blockId: blk_1073743482
 * }
 *
 * @see org.apache.hadoop.fs.BlockLocation
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("eagle_hdfs_blocks_snapshot")
@ColumnFamily("f")
@Prefix("hdfs_blocks_snapshot")
@Service(HDFSBlockSnapshotEntity.HDFS_BLOCK_LOCATION_SERVICE_NAME)
@TimeSeries(true)
@Partition({"site"})
@Tags({"site","blockpool","blockId","file"})

public class HDFSBlockSnapshotEntity extends TaggedLogAPIEntity {
    static final String HDFS_BLOCK_LOCATION_SERVICE_NAME = "HDFSBlockSnapshotService";
    @Column("a")
    private short blockReplication;
    @Column("b")
    private long blockSize;
    @Column("c")
    private boolean corrupt;
    @Column("d")
    private boolean underReplicated;
    @Column("e")
    private short currentReplication;
    @Column("f")
    private short missingReplication;
    @Column("g")
    private long modifiedTime;
    @Column("h")
    private long createdTime;
    @Column("i")
    private BlockLocation blockLocation;

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

    public short getBlockReplication() {
        return blockReplication;
    }

    public void setBlockReplication(short blockReplication) {
        this.blockReplication = blockReplication;
        valueChanged("blockReplication");
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
        valueChanged("blockSize");
    }

    public boolean isCorrupt() {
        return corrupt;
    }

    public void setCorrupt(boolean corrupt) {
        this.corrupt = corrupt;
        valueChanged("corrupt");
    }

    public boolean isUnderReplicated() {
        return underReplicated;
    }

    public void setUnderReplicated(boolean underReplicated) {
        this.underReplicated = underReplicated;
        valueChanged("underReplicated");
    }

    public short getCurrentReplication() {
        return currentReplication;
    }

    public void setCurrentReplication(short currentReplication) {
        this.currentReplication = currentReplication;
        valueChanged("currentReplication");
    }

    public short getMissingReplication() {
        return missingReplication;
    }

    public void setMissingReplication(short missingReplication) {
        this.missingReplication = missingReplication;
        valueChanged("missingReplication");
    }
}