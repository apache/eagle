import com.google.inject.Inject;
import org.apache.eagle.app.resource.ApplicationResource;
import org.apache.eagle.app.service.ApplicationOperations;
import org.apache.eagle.app.test.ApplicationTestBase;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.resource.SiteResource;
import org.apache.eagle.metadata.service.ApplicationStatusUpdateService;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2/20/17.
 */
public class HdfsMetricWebApplicationProviderTest extends ApplicationTestBase {
    @Inject
    private SiteResource siteResource;

    @Inject
    private ApplicationResource applicationResource;

    @Inject
    private ApplicationStatusUpdateService statusUpdateService;

    private void installDependencies(){
        ApplicationOperations.InstallOperation installDependency1 = new ApplicationOperations.InstallOperation("test_site", "TOPOLOGY_HEALTH_CHECK_APP", ApplicationEntity.Mode.LOCAL);
        applicationResource.installApplication(installDependency1);

        ApplicationOperations.InstallOperation installDependency2 = new ApplicationOperations.InstallOperation("test_site", "HADOOP_METRIC_MONITOR", ApplicationEntity.Mode.LOCAL);
        applicationResource.installApplication(installDependency2);
    }

    /**
     * register site
     * install app
     * start app
     * stop app
     * uninstall app
     *
     * @throws InterruptedException
     */
    @Test
    public void testApplicationLifecycle() throws InterruptedException {
        // Create local site
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("test_site");
        siteEntity.setSiteName("Test Site");
        siteEntity.setDescription("Test Site for HDFS_METRIC_WEB_APP");
        siteResource.createSite(siteEntity);
        Assert.assertNotNull(siteEntity.getUuid());



        ApplicationOperations.InstallOperation installOperation = new ApplicationOperations.InstallOperation("test_site", "HDFS_METRIC_WEB_APP", ApplicationEntity.Mode.LOCAL);
        installOperation.setConfiguration(getConf());
        installDependencies();
        // Install application
        ApplicationEntity applicationEntity = applicationResource.installApplication(installOperation).getData();
        //Todo: comment these for now, because they haven't been implemented
        // Start application
//        applicationResource.startApplication(new ApplicationOperations.StartOperation(applicationEntity.getUuid()));
//        // Stop application
//        applicationResource.stopApplication(new ApplicationOperations.StopOperation(applicationEntity.getUuid()));
        //Uninstall application
        awaitApplicationStop(applicationEntity);
        applicationResource.uninstallApplication(new ApplicationOperations.UninstallOperation(applicationEntity.getUuid()));
        try {
            applicationResource.getApplicationEntityByUUID(applicationEntity.getUuid());
            Assert.fail("Application instance (UUID: " + applicationEntity.getUuid() + ") should have been uninstalled");
        } catch (Exception ex) {
            // Expected exception
        }
    }

    private Map<String, Object> getConf() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("dataSinkConfig.topic", "testTopic");
        conf.put("dataSinkConfig.brokerList", "broker");
        conf.put("dataSinkConfig.serializerClass", "serializerClass");
        conf.put("dataSinkConfig.keySerializerClass", "keySerializerClass");
        conf.put("spoutNum", 2);
        conf.put("mode", "LOCAL");
        return conf;
    }
}
