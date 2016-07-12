package org.apache.eagle.metadata.service;

import com.google.inject.Provider;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.metadata.model.ApplicationSpec;
import org.apache.eagle.metadata.model.ApplicationsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ApplicationSpecServiceProvider implements Provider<ApplicationSpecService> {
    @Override
    public ApplicationSpecService get() {
        return new ApplicationSpecServiceImpl();
    }

    private static class ApplicationSpecServiceImpl implements ApplicationSpecService {
        public ApplicationSpecServiceImpl(){
            load();
        }

        private ApplicationsConfig applicationsConfig;
        private final static String DEFAULT_APPLICATIONS_CONFIG_FILE = "applications.xml";
        private final static String APPLICATIONS_CONFIG_PROPS_KEY = "applications.config";
        private final static Logger LOG = LoggerFactory.getLogger(ApplicationSpecServiceImpl.class);
        private final static Map<String,ApplicationSpec> APPLICATION_TYPE_SPEC_MAP = new HashMap<>();

        private void load() {
            try {
                APPLICATION_TYPE_SPEC_MAP.clear();
                Config config = ConfigFactory.load();
                String applicationsConfigFile = DEFAULT_APPLICATIONS_CONFIG_FILE;
                if(config.hasPath(APPLICATIONS_CONFIG_PROPS_KEY)){
                    applicationsConfigFile = config.getString(APPLICATIONS_CONFIG_PROPS_KEY);
                    LOG.info("Set {} = {}",APPLICATIONS_CONFIG_PROPS_KEY,applicationsConfigFile);
                }
                JAXBContext jc = JAXBContext.newInstance(ApplicationsConfig.class);
                Unmarshaller unmarshaller = jc.createUnmarshaller();
                InputStream is = ApplicationSpecServiceImpl.class.getResourceAsStream(applicationsConfigFile);
                if(is == null){
                    is = ApplicationSpecServiceImpl.class.getResourceAsStream("/"+applicationsConfigFile);
                }
                if(is == null){
                    LOG.error("Application configuration {} is not found",applicationsConfigFile);
                }
                applicationsConfig = (ApplicationsConfig) unmarshaller.unmarshal(is);
                for(ApplicationSpec applicationSpec:applicationsConfig.getApplications()){
                    APPLICATION_TYPE_SPEC_MAP.put(applicationSpec.getType(),applicationSpec);
                }
                LOG.info("Loaded {} applicationSpecs",applicationsConfig.getApplications().size());
            }catch (Exception ex){
                LOG.error("Failed to load application configuration: applications.xml",ex);
                throw new RuntimeException("Failed to load application configuration: applications.xml",ex);
            }
        }

        @Override
        public ApplicationSpec getApplicationSpecByType(String appType) {
            return APPLICATION_TYPE_SPEC_MAP.get(appType);
        }

        @Override
        public List<ApplicationSpec> getAllApplicationSpecs() {
            return applicationsConfig.getApplications();
        }
    }
}