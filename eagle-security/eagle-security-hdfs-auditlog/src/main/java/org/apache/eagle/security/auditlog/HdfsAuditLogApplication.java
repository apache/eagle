/*
 *
 *  *
 *  *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  *  * contributor license agreements.  See the NOTICE file distributed with
 *  *  * this work for additional information regarding copyright ownership.
 *  *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  *  * (the "License"); you may not use this file except in compliance with
 *  *  * the License.  You may obtain a copy of the License at
 *  *  * <p/>
 *  *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *  * <p/>
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */

package org.apache.eagle.security.auditlog;

import backtype.storm.topology.base.BaseRichBolt;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Since 8/11/16.
 */
public class HdfsAuditLogApplication extends AbstractHdfsAuditLogApplication {
    @Override
    public BaseRichBolt getParserBolt() {
        return new HdfsAuditLogParserBolt();
    }

    @Override
    public String getSinkStreamName() {
        return "hdfs_audit_log_stream";
    }

    public static void main(String[] args) {
        Config config = ConfigFactory.load();
        HdfsAuditLogApplication app = new HdfsAuditLogApplication();
        app.run(config);
    }
}
