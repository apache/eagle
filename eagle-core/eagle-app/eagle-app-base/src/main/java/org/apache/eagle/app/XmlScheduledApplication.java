package org.apache.eagle.app;

import com.typesafe.config.Config;
import org.apache.eagle.app.environment.impl.AbstractSchedulingPlan;
import org.apache.eagle.app.environment.impl.ScheduledEnvironment;
import org.apache.eagle.app.environment.impl.SchedulingPlan;
import org.quartz.SchedulerException;

/**
 * TODO: Support load scheduling from XML instead of in-line code.
 */
public class XmlScheduledApplication extends ScheduledApplication {
    private final String schedulingXmlFile;

    public XmlScheduledApplication(String schedulingXmlFile) {
        this.schedulingXmlFile = schedulingXmlFile;
    }

    @Override
    public SchedulingPlan execute(Config config, ScheduledEnvironment environment) {
         return new XmlSchedulingPlan(schedulingXmlFile,config, environment);
    }

    private class XmlSchedulingPlan extends AbstractSchedulingPlan {
        public XmlSchedulingPlan(String schedulingXmlFile, Config config, ScheduledEnvironment environment) {
            super(config, environment);
            throw new RuntimeException("Just proposal, not implemented yet");
        }

        @Override
        public String getAppId() {
            return null;
        }

        @Override
        public void schedule() throws SchedulerException {

        }

        @Override
        public boolean unschedule() throws SchedulerException {
            return false;
        }

        @Override
        public boolean scheduling() throws SchedulerException {
            return false;
        }
    }
}