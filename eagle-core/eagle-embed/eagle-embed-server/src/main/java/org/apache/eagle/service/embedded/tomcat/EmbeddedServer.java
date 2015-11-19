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
package org.apache.eagle.service.embedded.tomcat;

import java.io.File;

import org.apache.catalina.LifecycleState;
import org.apache.catalina.startup.Tomcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedServer {
	
	private static EmbeddedServer server;
	private Tomcat tomcat;
	private String webappDirLocation;
	private int port;
	private static int DEFAULT_TOMCAT_PORT = 38080;
	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedServer.class);
		
	public static void main(String[] args) {
        // args: webappLocation, port
        int tomcatPort;
        if (args.length > 1) {
            tomcatPort = Integer.valueOf(args[1]);
        } else {
            tomcatPort = DEFAULT_TOMCAT_PORT;
        }
        new EmbeddedServer(args[0], tomcatPort).start();
	}

	private EmbeddedServer(String webappDirLocation) {
		this(webappDirLocation, DEFAULT_TOMCAT_PORT);
	}
	
	private EmbeddedServer(String webappDirLocation, int port) {
		this.webappDirLocation = webappDirLocation;
		this.port = port;
	}
	
    public static EmbeddedServer getInstance(String webappDirLocation) {
    	if (server == null) {
    		synchronized(EmbeddedServer.class) {
    			if (server == null) {
    				server = new EmbeddedServer(webappDirLocation);
    				server.start();   						
    			}
    		}
    	}
    	return server;
    }

	public int getPort() {
		return port;
	}
	
	public void start() {
		tomcat = new Tomcat();
		tomcat.setHostname("localhost");
		tomcat.setPort(port);
		try {
			tomcat.addWebapp("/eagle-service", new File(webappDirLocation).getAbsolutePath());
			tomcat.start();

		} catch (Exception ex) {
			LOG.error("Got an exception " + ex.getMessage());
		}

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
            	try {            		
            		shutdown();
            	}
            	catch (Throwable t) {
            		LOG.error("Got an exception why shutting down..." + t.getMessage());
            	}
            }
        });
		try {
			Thread.sleep(10000000);
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	public void shutdown() throws Throwable {
	  	if (tomcat.getServer() != null && tomcat.getServer().getState() != LifecycleState.DESTROYED) {
	        if (tomcat.getServer().getState() != LifecycleState.STOPPED) {
	        	tomcat.stop();
	        }
	        tomcat.destroy();
	    }
	}
}
