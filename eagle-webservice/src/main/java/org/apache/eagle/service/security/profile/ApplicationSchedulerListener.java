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

package org.apache.eagle.service.security.profile;


import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.stream.application.scheduler.ApplicationScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.concurrent.TimeUnit;

public class ApplicationSchedulerListener implements ServletContextListener {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSchedulerListener.class);

    //@Autowired
    private ActorSystem system;

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        //Get the actor system from the spring context
        //SpringBeanAutowiringSupport.processInjectionBasedOnCurrentContext(this);
        Config config = ConfigFactory.load("eagleScheduler.conf");
        system = new ApplicationScheduler().start(config);
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        if (system != null) {
            LOG.info("Killing ActorSystem as a part of web application ctx destruction.");
            system.shutdown();
            system.awaitTermination(Duration.create(15, TimeUnit.SECONDS));
        } else {
            LOG.warn("No actor system loaded, yet trying to shut down. Check AppContext config and consider if you need this listener.");
        }
    }

}
