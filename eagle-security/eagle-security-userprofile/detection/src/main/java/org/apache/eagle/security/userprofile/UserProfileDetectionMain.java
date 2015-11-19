package org.apache.eagle.security.userprofile;

import com.typesafe.config.Config;
import org.apache.eagle.dataproc.util.ConfigOptionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UserProfileDetectionMain {
    private final static Logger LOG = LoggerFactory.getLogger(UserProfileDetectionMain.class);
    public final static String USERPROFILE_DETECTION_MODE_KEY="eagleProps.userProfileMode";
    public static void main(String[] args) throws Exception {
        Config config = new ConfigOptionParser().load(args);
        LOG.info("Config class: " + config.getClass().getCanonicalName());

        if(config.hasPath(USERPROFILE_DETECTION_MODE_KEY) && config.getString(USERPROFILE_DETECTION_MODE_KEY).equalsIgnoreCase("batch")){
            LOG.info("Starting UserProfileDetection Topology in [batch] mode");
            UserProfileDetectionBatchMain.main(args);
        }else{
            LOG.info("Starting UserProfileDetection Topology in [streaming] mode");
            UserProfileDetectionStreamMain.main(args);
        }
    }
}