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
package eagle.service.client;

import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eagle.service.embedded.tomcat.EmbeddedServer;
import eagle.service.hbase.EmbeddedHbase;

public class ClientTestBase {
	
	protected static EmbeddedServer server;
	protected static EmbeddedHbase hbase;

	@BeforeClass
	public static void startup() throws Exception {
		hbase = EmbeddedHbase.getInstance();
		String webappDirLocation = "../../../eagle-webservice/target/eagle-service";
		server = EmbeddedServer.getInstance(webappDirLocation);
	}
	
//	//@AfterClass
//	@After
//	public static void teardown()  {
//		try {
//			server.stop();
//			hbase.shutdown();
//		} catch (Throwable t) {
//			LOG.error("Got an throwable t, " + t.getMessage());
//		}
//		finally {
//			try {
//				Thread.sleep(3000);
//			}
//			catch (Exception ex) {}
//		}
//	}
}
