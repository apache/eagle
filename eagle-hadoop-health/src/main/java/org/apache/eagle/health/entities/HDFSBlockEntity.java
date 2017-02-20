/**
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
package org.apache.eagle.health.entities;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("eagle_hdfs_metadata")
@ColumnFamily("f")
@Prefix("hdfs_blocks_snapshot")
@Service(HDFSBlockEntity.HDFS_BLOCK_SERVICE_NAME)
@TimeSeries(false)
@Partition({"site"})
@Tags({"site","path","blockId", "poolId", "owner", "group"})
public class HDFSBlockEntity extends TaggedLogAPIEntity implements Serializable {
    public static final String HDFS_BLOCK_SERVICE_NAME = "HDFSBlockService";
    @Column("a")
    private String[] racks;
    @Column("b")
    private String[] hosts;
    @Column("c")
    private String[] ipAddresses;
    @Column("d")
    private DatanodeInfo.AdminStates[] states;
    @Column("e")
    private String[] storageIds;
    @Column("f")
    private StorageType[] storageTypes;
    @Column("g")
    private long generationStamp;
    @Column("h")
    private boolean corrupt;
    @Column("i")
    private boolean underReplicated;
    @Column("j")
    private int actualReplicas;
    @Column("k")
    private int missingReplicas;
    @Column("l")
    private int blockReplication;
    @Column("m")
    private Boolean isdir;
    @Column("n")
    private long blockSize;
    @Column("o")
    private String[] lastRacks;
    @Column("p")
    private String[] lastHosts;
    @Column("q")
    private String[] lastIpAddresses;
    @Column("r")
    private long modifiedTime;

    public HDFSBlockEntity() {}

    public HDFSBlockEntity(String siteId, FileStatus status, LocatedBlock block, boolean underReplicated, long timestamp) {
        Map<String,String> tags = new HashMap<>();
        tags.put("site", siteId);
        tags.put("path", status.getPath().toString());
        tags.put("blockId", block.getBlock().getBlockName());
        tags.put("poolId", block.getBlock().getBlockPoolId());
        tags.put("owner", status.getOwner());
        tags.put("group", status.getGroup());
        this.setTags(tags);

        this.setCorrupt(block.isCorrupt());

        int locationNum = block.getLocations().length;
        this.setRacks(new String[locationNum]);
        this.setHosts(new String[locationNum]);
        this.setStates(new DatanodeInfo.AdminStates[locationNum]);
        this.setIpAddresses(new String[locationNum]);
        for (int i = 0; i < locationNum; i++) {
            this.hosts[i] = block.getLocations()[i].getHostName();
            this.racks[i] = block.getLocations()[i].getNetworkLocation();
            this.states[i] = block.getLocations()[i].getAdminState();
            this.ipAddresses[i] = block.getLocations()[i].getIpAddr();
        }
        this.setStorageIds(block.getStorageIDs());
        this.setStorageTypes(block.getStorageTypes());

        this.setBlockReplication(status.getReplication());
        this.setBlockSize(block.getBlockSize());
        this.setIsdir(status.isDirectory());
        this.setUnderReplicated(underReplicated);
        this.setActualReplicas(block.getLocations().length);
        this.setMissingReplicas(status.getReplication() - this.getActualReplicas());
        this.setGenerationStamp(block.getBlock().getGenerationStamp());

        this.setModifiedTime(timestamp);
        this.setTimestamp(timestamp);

        // Avoid erase all location information when corrupt.

        if (!ArrayUtils.isEmpty(this.getHosts())) {
            this.setLastHosts(this.getHosts());
        }
        if (!ArrayUtils.isEmpty(this.getRacks())) {
            this.setLastRacks(this.getRacks());
        }
        if (!ArrayUtils.isEmpty(this.getIpAddresses())) {
            this.setLastIpAddresses(this.getIpAddresses());
        }
    }

    public String[] getRacks() {
        return racks;
    }

    public void setRacks(String[] racks) {
        this.racks = racks;
        valueChanged("racks");
    }

    public boolean isCorrupt() {
        return corrupt;
    }

    public void setCorrupt(boolean corrupt) {
        this.corrupt = corrupt;
        valueChanged("corrupt");
    }

    public String[] getHosts() {
        return hosts;
    }

    public void setHosts(String[] hosts) {
        this.hosts = hosts;
        valueChanged("hosts");
    }

    public String[] getIpAddresses() {
        return ipAddresses;
    }

    public void setIpAddresses(String[] ipAddresses) {
        this.ipAddresses = ipAddresses;
        valueChanged("ipAddresses");
    }

    public DatanodeInfo.AdminStates[] getStates() {
        return states;
    }

    public void setStates(DatanodeInfo.AdminStates[] states) {
        this.states = states;
        valueChanged("states");
    }

    public int getActualReplicas() {
        return actualReplicas;
    }

    public void setActualReplicas(int actualReplicas) {
        this.actualReplicas = actualReplicas;
        valueChanged("actualReplicas");
    }

    public int getMissingReplicas() {
        return missingReplicas;
    }

    public void setMissingReplicas(int missingReplicas) {
        this.missingReplicas = missingReplicas;
        valueChanged("missingReplicas");
    }

    public boolean isUnderReplicated() {
        return underReplicated;
    }

    public void setUnderReplicated(boolean underReplicated) {
        this.underReplicated = underReplicated;
        valueChanged("underReplicated");
    }

    public int getBlockReplication() {
        return blockReplication;
    }

    public void setBlockReplication(int blockReplication) {
        this.blockReplication = blockReplication;
        valueChanged("blockReplication");
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(long modifiedTime) {
        this.modifiedTime = modifiedTime;
        valueChanged("modifiedTime");
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
        valueChanged("blockSize");
    }

    public Boolean getIsdir() {
        return isdir;
    }

    public void setIsdir(Boolean isdir) {
        this.isdir = isdir;
        valueChanged("isdir");
    }

    public String[] getLastRacks() {
        return lastRacks;
    }

    public void setLastRacks(String[] lastRacks) {
        this.lastRacks = lastRacks;
        valueChanged("lastRacks");
    }

    public String[] getLastHosts() {
        return lastHosts;
    }

    public void setLastHosts(String[] lastHosts) {
        this.lastHosts = lastHosts;
        valueChanged("lastHosts");
    }

    public String[] getLastIpAddresses() {
        return lastIpAddresses;
    }

    public void setLastIpAddresses(String[] lastIpAddresses) {
        this.lastIpAddresses = lastIpAddresses;
        valueChanged("lastIpAddresses");
    }

    public StorageType[] getStorageTypes() {
        return storageTypes;
    }

    public void setStorageTypes(StorageType[] storageTypes) {
        this.storageTypes = storageTypes;
        valueChanged("storageTypes");
    }

    public String[] getStorageIds() {
        return storageIds;
    }

    public void setStorageIds(String[] storageIds) {
        this.storageIds = storageIds;
        valueChanged("storageIds");
    }

    public long getGenerationStamp() {
        return generationStamp;
    }

    public void setGenerationStamp(long generationStamp) {
        this.generationStamp = generationStamp;
        valueChanged("generationStamp");
    }
}
