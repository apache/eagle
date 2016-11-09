/*
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
package org.apache.eagle.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Version {
    public static final String projectName = "Eagle";
    public static final String moduleName = "${project.name}";
    public static final String version = "${project.version}";
    public static final String buildNumber = "${buildNumber}";
    public static String gitRevision = "${revision}";
    public static final String userName = "${user.name}";
    public static final String timestamp = "${timestamp}";

    private static final Logger LOG = LoggerFactory.getLogger(Version.class);

    static {
        InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("build.properties");
        Properties p = new Properties();
        try {
            p.load(resourceAsStream);
            gitRevision = p.getProperty("revision", "unknown");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            LOG.info("Eagle {} {}",Version.version, new Date(Long.parseLong(Version.timestamp)));
        }
    }

    @Override
    public String toString() {
        return String.format("%s version=%s buildNumber=%s gitRevision=%s built by %s on %s",
            projectName, version, buildNumber, gitRevision, userName, new Date(Long.parseLong(timestamp)));
    }

    public static String str() {
        return new Version().toString();
    }
}
