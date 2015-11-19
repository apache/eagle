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
package org.apache.eagle.service.client;

import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.apache.eagle.service.client.impl.ConcurrentSender;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TestEagleServiceClientImpl extends ClientTestBase {

    IEagleServiceClient client = new EagleServiceClientImpl("localhost",38080);

    //@Before
    public void setUp() {
        hbase.createTable("unittest", "f");
    }

    //After
    public void cleanUp() {
        hbase.deleteTable("unittest");
    }

    //@Test
    public void testCreateAndSearch() throws IOException, EagleServiceClientException, IllegalAccessException, InstantiationException {
        List<TestTimeSeriesAPIEntity> entities = new ArrayList<TestTimeSeriesAPIEntity>();

        for(int i=0;i<100;i++){
            TestTimeSeriesAPIEntity entity = new TestTimeSeriesAPIEntity();
            entity.setTimestamp(System.currentTimeMillis());
            entity.setTags(new HashMap<String, String>() {{
                put("cluster", "cluster4ut");
                put("datacenter", "datacenter4ut");
                put("timestampStr",System.currentTimeMillis()+"");
            }});
            entity.setField1(1);
            entity.setField2(1);
            entity.setField3(1);
            entity.setField4(1l);
            entity.setField5(1l);
            entity.setField5(1.2);
            entity.setField6(-1.2);
            entity.setField7("test unit string attribute");
            entities.add(entity);
        }

        GenericServiceAPIResponseEntity response = client.create(entities);
        assert response.isSuccess();
        response = client.create(entities,TestTimeSeriesAPIEntity.class);
        assert response.isSuccess();
        response = client.create(entities,"TestTimeSeriesAPIEntity");
        assert response.isSuccess();

        response = client.search("TestTimeSeriesAPIEntity[]{*}")
                .startTime(0)
                .endTime(System.currentTimeMillis() + 25 * 3600 * 1000)
                .pageSize(1000)
                .send();

        assert  response.isSuccess();
        assert response.getObj().size() > 0;
    }

    private TestTimeSeriesAPIEntity newEntity(){
        TestTimeSeriesAPIEntity entity = new TestTimeSeriesAPIEntity();
        entity.setTimestamp(System.currentTimeMillis());
        entity.setTags(new HashMap<String, String>() {{
            put("cluster", "cluster4ut");
            put("datacenter", "datacenter4ut");
        }});
        entity.setField1(1);
        entity.setField2(1);
        entity.setField3(1);
        entity.setField4(1l);
        entity.setField5(1l);
        entity.setField5(1.2);
        entity.setField6(-1.2);
        entity.setField7("test unit string attribute");
        return entity;
    }

    //@Test
    public void testUpdate() throws IOException, EagleServiceClientException, IllegalAccessException, InstantiationException {
        List<TestTimeSeriesAPIEntity> entities = new ArrayList<TestTimeSeriesAPIEntity>();
        for(int i=0;i<100;i++){
            TestTimeSeriesAPIEntity entity = new TestTimeSeriesAPIEntity();
            entity.setTimestamp(System.currentTimeMillis());
            entity.setTags(new HashMap<String, String>() {{
                put("cluster", "cluster4ut");
                put("datacenter", "datacenter4ut");
            }});
            entity.setField1(1);
            entity.setField2(1);
            entity.setField3(1);
            entity.setField4(1l);
            entity.setField5(1l);
            entity.setField5(1.2);
            entity.setField6(-1.2);
            entity.setField7("updated");
            entities.add(entity);
        }

        GenericServiceAPIResponseEntity response = client.update(entities);
        assert response.isSuccess();
        response = client.update(entities, TestTimeSeriesAPIEntity.class);
        assert response.isSuccess();
        response = client.update(entities, "TestTimeSeriesAPIEntity");
        assert response.isSuccess();

        response = client.search("TestTimeSeriesAPIEntity[]{*}")
                .startTime(0)
                .endTime(System.currentTimeMillis() + 25 * 3600 * 1000)
                .pageSize(1000)
                .send();

        assert response.isSuccess();
        assert response.getObj().size() > 0;
    }

    //@Test
    public void testDelete() throws IOException, EagleServiceClientException {
        List<TestTimeSeriesAPIEntity> entities = new ArrayList<TestTimeSeriesAPIEntity>();
        for(int i=0;i<100;i++){
            TestTimeSeriesAPIEntity entity = new TestTimeSeriesAPIEntity();
            entity.setTimestamp(System.currentTimeMillis());
            entity.setTags(new HashMap<String, String>() {{
                put("cluster", "cluster4ut");
                put("datacenter", "datacenter4ut");
            }});

            entity.setField1(1);
            entity.setField2(1);
            entity.setField3(1);
            entity.setField4(1l);
            entity.setField5(1l);
            entity.setField5(1.2);
            entity.setField6(-1.2);
            entity.setField7(" unit test oriented string attribute");
            entities.add(entity);
        }

        GenericServiceAPIResponseEntity response = client.delete(entities);
        assert response.isSuccess();
        response = client.delete(entities, TestTimeSeriesAPIEntity.class);
        assert response.isSuccess();
        response = client.delete(entities, "TestTimeSeriesAPIEntity");
        assert response.isSuccess();

        response = client.delete()
                .byId(Arrays.asList("30RR1H___rOqxUr5M_sR-g5RxZlmldR_9eQ49A"))
                .serviceName("TestTimeSeriesAPIEntity")
                .send();

        assert response.isSuccess();

        response = client.delete()
                .byQuery("TestTimeSeriesAPIEntity[]{*}")
                .startTime(0)
                .endTime(System.currentTimeMillis())
                .pageSize(1000)
                .send();

        assert response.isSuccess();
    }

    //@Test
    public void testMetricsSender() throws IOException, EagleServiceClientException {
        List<GenericMetricEntity> entities = new ArrayList<GenericMetricEntity>();

        Map<String,String> tags = new HashMap<String, String>() {{
            put("cluster", "cluster4ut");
            put("datacenter", "datacenter4ut");
        }};

        for(int i=0;i<100;i++){
            GenericMetricEntity entity = new GenericMetricEntity();
            entity.setTimestamp(System.currentTimeMillis());
            entity.setTags(tags);
            entity.setValue(new double[]{1.234});
            entity.setPrefix("unit.test.metrics");
            entities.add(entity);
        }

        GenericServiceAPIResponseEntity response = client.create(entities);
        assert response.isSuccess();
        response = client.create(entities,GenericMetricEntity.class);
        assert response.isSuccess();
        response = client.create(entities,GenericMetricEntity.GENERIC_METRIC_SERVICE);
        assert response.isSuccess();

        client.metric("unit.test.metrics")
                .batch(5)
                .tags(tags)
                .send("unit.test.anothermetrics", System.currentTimeMillis(), tags, 0.1, 0.2, 0.3)
                .send(System.currentTimeMillis(), 0.1)
                .send(System.currentTimeMillis(),0.1,0.2)
                .send(System.currentTimeMillis(),0.1,0.2,0.3)
                .send(System.currentTimeMillis(),tags,0.1,0.2,0.3)
                .send("unit.test.anothermetrics",System.currentTimeMillis(),tags,0.1,0.2,0.3)
                .flush();

        GenericServiceAPIResponseEntity<GenericMetricEntity> metricResponse = client.search("GenericMetricService[@cluster=\"cluster4ut\" AND @datacenter = \"datacenter4ut\"]{*}")
                .startTime(0)
                .endTime(System.currentTimeMillis()+24 * 3600 * 1000)
                .metricName("unit.test.metrics")
                .pageSize(1000)
                .send();
        List<GenericMetricEntity> metricEntities = metricResponse.getObj();
        assert metricEntities != null;
        assert metricResponse.isSuccess();

        GenericServiceAPIResponseEntity<Map> metricAggResponse = client.search("GenericMetricService[@cluster=\"cluster4ut\" AND @datacenter = \"datacenter4ut\"]<@cluster>{sum(value)}")
                .startTime(0)
                .endTime(System.currentTimeMillis()+24 * 3600 * 1000)
                .metricName("unit.test.metrics")
                .pageSize(1000)
                .send();
        List<Map> aggResult = metricAggResponse.getObj();
        assert aggResult != null;
        assert metricAggResponse.isSuccess();

        client.close();
    }

    //@Test
    public void testBatchSender() throws IOException, EagleServiceClientException {
        client.batch(2)
                .send(newEntity())
                .send(newEntity())
                .send(newEntity());
        client.close();
    }

    //@Test
    public void testAsyncSender() throws IOException, EagleServiceClientException, ExecutionException, InterruptedException {
        EagleServiceAsyncClient asyncClient = client.async();

        Future<GenericServiceAPIResponseEntity<String>> future1 =
                asyncClient.create(Arrays.asList(newEntity()));

        GenericServiceAPIResponseEntity<String> response1 = future1.get();

        Assert.assertTrue(response1.isSuccess());

        Future<GenericServiceAPIResponseEntity<String>> future2 =
                asyncClient.update(Arrays.asList(newEntity()));

        GenericServiceAPIResponseEntity<String> response2 = future2.get();

        Assert.assertTrue(response2.isSuccess());

        Future<GenericServiceAPIResponseEntity<String>> future3 =
                asyncClient.delete(Arrays.asList(newEntity()));

        GenericServiceAPIResponseEntity<String> response3 = future3.get();

        Assert.assertTrue(response3.isSuccess());

        client.close();
    }

    //@Test
    public void testParallelSender() throws IOException, EagleServiceClientException, InterruptedException {
        // Case #1:
        ConcurrentSender concurrentSender = client
                .parallel(10)
                .batchSize(30)
                .batchInterval(1000);

        int num = 1000;

        for(int i=0; i< num;i++) {
            concurrentSender.send(Arrays.asList(newEntity()));
        }

        // Case #2:
        ConcurrentSender concurrentSender2 = client
                .parallel(10)
                .batchSize(20)
                .batchInterval(3);

        int num2 = 50;

        for(int i=0; i< num2;i++) {
            concurrentSender2.send(Arrays.asList(newEntity()));
            Thread.sleep(1);
        }
        client.close();
    }

    //@Test
    public void testSearch() throws EagleServiceClientException, IOException {
        hbase.createTable("eagle_metric", "f");

        GenericServiceAPIResponseEntity<TestTimeSeriesAPIEntity> response =
                client.search("TestTimeSeriesAPIEntity[]{*}").startTime(0).endTime(System.currentTimeMillis()+1000).pageSize(1000).send();

        Assert.assertTrue(response.isSuccess());

        GenericServiceAPIResponseEntity<Map> response2 =
                client.search("TestTimeSeriesAPIEntity[]<@cluster>{count}").startTime(0).endTime(System.currentTimeMillis()+1000).pageSize(1000).send();

        Assert.assertTrue(response2.isSuccess());

        GenericServiceAPIResponseEntity<GenericMetricEntity> response3 =
                client.search("GenericMetricService[@cluster = \"cluster4ut\" AND @datacenter = \"datacenter4ut\"]{*}").metricName("unit.test.metrics").startTime(0).endTime(System.currentTimeMillis()+1000).pageSize(1000).send();

        Assert.assertTrue(response3.isSuccess());
        hbase.deleteTable("eagle_metric");
    }

    @Test
    public void test() {

    }

}