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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoDatabase;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public abstract class MongoDependencyBaseTest {

	private static Logger LOG = LoggerFactory.getLogger(MongoDependencyBaseTest.class);
	
	private static SimpleEmbedMongo mongo;
    @SuppressWarnings("unused")
	private static MongoDatabase testDB;
    private static Config config;

    protected static MongoDedupEventsStore store;
    
    public static void before() {
        try {
            mongo = new SimpleEmbedMongo();
            mongo.start();
            testDB = mongo.getMongoClient().getDatabase("testDb");
        } catch (Exception e) {
            LOG.error("start embed mongod failed, assume some external mongo running. continue run test!", e);
        }
    }

    @BeforeClass
    public static void setup() throws Exception {
        before();

        System.setProperty("config.resource", "/application-mongo-statestore.conf");
        ConfigFactory.invalidateCaches();
        config = ConfigFactory.load();
        
        store = new MongoDedupEventsStore(config, "testPublishment");
    }

    @AfterClass
    public static void teardown() {
        if (mongo != null) {
            mongo.shutdown();
        }
    }
	
}
