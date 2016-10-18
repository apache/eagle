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
package org.apache.eagle.storage.hbase.it;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import java.io.File;
import java.util.Collection;

public class CoprocessorJarUtils {
    public static File getCoprocessorJarFile() {
        String projectRootDir = System.getProperty("user.dir");
        String targetDirPath = projectRootDir + "/target/";
        File targetDirFile = new File(targetDirPath);
        if (!targetDirFile.exists()) {
            throw new IllegalStateException(targetDirPath + " not found, please execute 'mvn install -DskipTests' under " + projectRootDir + " to build the project firstly and retry");
        }
        String jarFileNameWildCard = "eagle-storage-hbase-*-coprocessor.jar";
        Collection<File> jarFiles = FileUtils.listFiles(targetDirFile, new WildcardFileFilter(jarFileNameWildCard), TrueFileFilter.INSTANCE);
        if (jarFiles.size() == 0) {
            throw new IllegalStateException("jar is not found, please execute 'mvn package -DskipTests' from project root firstly and retry");
        }
        return jarFiles.iterator().next();
    }
}
