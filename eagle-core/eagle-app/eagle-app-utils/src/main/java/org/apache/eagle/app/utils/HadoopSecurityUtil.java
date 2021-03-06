/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.app.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * This class provides util methods for Eagle connector communicating
 * with secured cluster.
 */
public class HadoopSecurityUtil {

    public static final String EAGLE_KEYTAB_FILE_KEY = "eagle.keytab.file";
    public static final String EAGLE_PRINCIPAL_KEY = "eagle.kerberos.principal";

    public static void login(Configuration kConfig) throws IOException {
        String keytab = kConfig.get(EAGLE_KEYTAB_FILE_KEY);
        String principal = kConfig.get(EAGLE_PRINCIPAL_KEY);
        if ( keytab == null ||  principal == null || keytab.isEmpty() || principal.isEmpty()) {
            return;
        }

        kConfig.setBoolean("hadoop.security.authorization", true);
        kConfig.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(kConfig);
        UserGroupInformation.loginUserFromKeytab(kConfig.get(EAGLE_PRINCIPAL_KEY), kConfig.get(EAGLE_KEYTAB_FILE_KEY));
    }

}