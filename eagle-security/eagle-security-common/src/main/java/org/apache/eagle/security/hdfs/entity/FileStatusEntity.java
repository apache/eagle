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
package org.apache.eagle.security.hdfs.entity;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class FileStatusEntity {
    //private Path path;
    private long length;
    private boolean isdir;
    private short block_replication;
    private long blocksize;
    private long modification_time;

    private long access_time;
    private FsPermission permission;
    private String owner;
    private String group;
    private Path symlink;
    private String resource;
    private String sensitiveType;
    private Set<String> childSensitiveTypes;

    public FileStatusEntity(){

    }

    public FileStatusEntity(FileStatus status) throws IOException {
        //this.path = status.getPath();
        this.length = status.getLen();
        this.isdir = status.isDirectory();
        this.block_replication = status.getReplication();
        this.blocksize = status.getBlockSize();
        this.modification_time = status.getModificationTime();
        this.access_time = status.getAccessTime();
        this.permission = status.getPermission();
        this.owner = status.getOwner();
        this.group = status.getGroup();
        if(status.isSymlink()) {
            this.symlink = status.getSymlink();
        }
    }

    public long getAccess_time() {
        return access_time;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public void setAccess_time(long access_time) {
        this.access_time = access_time;

    }

    //public Path getPath() {
    //    return path;
    //}

    //public void setPath(Path path) {
    //    this.path = path;
    //}

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public boolean isIsdir() {
        return isdir;
    }

    public void setIsdir(boolean isdir) {
        this.isdir = isdir;
    }

    public short getBlock_replication() {
        return block_replication;
    }

    public void setBlock_replication(short block_replication) {
        this.block_replication = block_replication;
    }

    public long getBlocksize() {
        return blocksize;
    }

    public void setBlocksize(long blocksize) {
        this.blocksize = blocksize;
    }

    public long getModification_time() {
        return modification_time;
    }

    public void setModification_time(long modification_time) {
        this.modification_time = modification_time;
    }

    public FsPermission getPermission() {
        return permission;
    }

    public void setPermission(FsPermission permission) {
        this.permission = permission;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Path getSymlink() {
        return symlink;
    }

    public void setSymlink(Path symlink) {
        this.symlink = symlink;
    }

    public String getSensitiveType() {
        return sensitiveType;
    }

    public void setSensitiveType(String sensitiveType) {
        this.sensitiveType = sensitiveType;
    }

    public Set<String> getChildSensitiveTypes() {
        return childSensitiveTypes;
    }

    public void setChildSensitiveTypes(Set<String> childSensitiveTypes) {
        this.childSensitiveTypes = childSensitiveTypes;
    }
    
}