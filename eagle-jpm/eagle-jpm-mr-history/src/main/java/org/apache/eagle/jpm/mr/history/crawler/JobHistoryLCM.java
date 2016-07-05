/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.mr.history.crawler;

import org.apache.commons.lang3.tuple.Pair;

import java.io.InputStream;
import java.util.List;

/**
 * Define various operations on job history file resource for lifecycle management
 *
 * The job history file directory structure supported is as follows:
 * <basePath>/<jobTrackerName>/<year>/<month>/<day>/<serialNumber>/<jobHistoryFileName>
 *
 * In some hadoop version, <jobTrackerName> is not included
 *
 * The operations involved in resource read
 * - list job tracker names under basePath (mostly basePath is configured in entry mapreduce.jobhistory.done-dir of mapred-site.xml)
 * - list serial numbers under one day
 * - list job history files under one serial number
 * - read one job history file
 *
 */
public interface JobHistoryLCM {
    String calculateJobTrackerName(String basePath) throws Exception;
    /**
     * @param year
     * @param month 0-based or 1-based month depending on hadoop cluster setting
     * @param day
     * @return
     * @throws Exception
     */
    List<String> readSerialNumbers(int year, int month, int day) throws Exception;
    /**
     * @param year
     * @param month 0-based or 1-based month depending on hadoop cluster setting
     * @param day
     * @param serialNumber
     * @return
     * @throws Exception
     */
    List<Pair<Long, String> > readFileNames(int year, int month, int day, int serialNumber) throws Exception;
    /**
     * @param year
     * @param month 0-based or 1-based month depending on hadoop cluster setting
     * @param day
     * @param serialNumber
     * @param jobHistoryFileName
     * @param reader
     * @throws Exception
     */
    void readFileContent(int year, int month, int day, int serialNumber, String jobHistoryFileName, JHFInputStreamCallback reader) throws Exception;
    /**
     * @param year
     * @param month 0-based or 1-based month depending on hadoop cluster setting
     * @param day
     * @param serialNumber
     * @param jobHistoryFileName
     * @return
     * @throws Exception
     */
    InputStream getJHFFileContentAsStream(int year, int month, int day, int serialNumber, String jobHistoryFileName) throws Exception;
    InputStream getJHFConfContentAsStream(int year, int month, int day, int serialNumber, String jobConfFileName) throws Exception;

    /**
     *
     */
    void freshFileSystem() throws Exception;
}
