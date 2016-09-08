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
package org.apache.eagle.alert.engine.publisher.dedup;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class SimpleEmbedMongo {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleEmbedMongo.class);
    
    private MongoClient client;
    private MongodExecutable mongodExe;
    private MongodProcess mongod;

    public void start() throws Exception {
        MongodStarter starter = MongodStarter.getDefaultInstance();
        mongodExe = starter.prepare(new MongodConfigBuilder().version(Version.V3_2_1)
                .net(new Net(27017, Network.localhostIsIPv6())).build());
        mongod = mongodExe.start();

        client = new MongoClient("localhost");
    }

    public void shutdown() {

        if (mongod != null) {
            try {
                mongod.stop();
            }
            catch (IllegalStateException e) {
                // catch this exception for the unstable stopping mongodb
                // reason: the exception is usually thrown out with below message format when stop() returns null value,
                //         but actually this should have been captured in ProcessControl.stopOrDestroyProcess() by destroying
                //         the process ultimately
                if (e.getMessage() != null && e.getMessage().matches("^Couldn't kill.*process!.*")) {
                    // if matches, do nothing, just ignore the exception
                } else {
                    LOG.warn(String.format("Ignored error for stopping mongod process, see stack trace: %s", ExceptionUtils.getStackTrace(e)));
                }
            }
            mongodExe.stop();
        }
    }

    public MongoClient getMongoClient() {
        return client;
    }

	
}
