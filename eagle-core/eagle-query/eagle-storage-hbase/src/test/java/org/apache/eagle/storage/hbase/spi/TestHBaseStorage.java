package org.apache.eagle.storage.hbase.spi;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.service.hbase.TestHBaseBase;
import org.apache.eagle.storage.DataStorage;
import org.apache.eagle.storage.DataStorageManager;
import org.apache.eagle.storage.exception.QueryCompileException;
import org.apache.eagle.storage.operation.CompiledQuery;
import org.apache.eagle.storage.operation.RawQuery;
import org.apache.eagle.storage.result.ModifyResult;
import org.apache.eagle.storage.result.QueryResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class TestHBaseStorage extends TestHBaseBase {

    static final Logger LOG = LoggerFactory.getLogger(TestHBaseStorage.class);
    EntityDefinition entityDefinition;
    DataStorage<String> storage;
    long baseTimestamp;

    private TestTimeSeriesAPIEntity newInstance() {
        TestTimeSeriesAPIEntity instance = new TestTimeSeriesAPIEntity();
        instance.setField1(123);
        instance.setField2(234);
        instance.setField3(1231312312L);
        instance.setField4(12312312312L);
        instance.setField5(123123.12312);
        instance.setField6(-12312312.012);
        instance.setField7(UUID.randomUUID().toString());
        instance.setTags(new HashMap<String, String>() {
            {
                put("cluster", "c4ut");
                put("datacenter", "d4ut");
                put("random", UUID.randomUUID().toString());
            }
        });
        instance.setTimestamp(System.currentTimeMillis());
        return instance;
    }

    @Before
    public void setUp() throws Exception {
        entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class);
        entityDefinition.setTags(new String[] {"cluster", "datacenter", "random"});

        storage = DataStorageManager.getDataStorageByEagleConfig();
        storage.init();
        GregorianCalendar gc = new GregorianCalendar();
        gc.clear();
        gc.set(2014, 1, 6, 1, 40, 12);
        gc.setTimeZone(TimeZone.getTimeZone("UTC"));
        baseTimestamp = gc.getTime().getTime();
        LOG.info("timestamp: {}" , baseTimestamp);
    }

    @Test
    public void testReadBySimpleQuery() throws QueryCompileException, IOException {
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[@cluster=\"c4ut\"]{*}");
        System.out.println(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp));
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithMilliseconds(baseTimestamp + 2000));
        rawQuery.setPageSize(1000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        QueryResult<TestTimeSeriesAPIEntity> result = storage.query(query, entityDefinition);
        Assert.assertNotNull(result);
    }

    @Test
    public void testReadByNotEqualCondition() throws QueryCompileException, IOException {
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[@cluster!=\"c4ut_not_found\" AND @field1 != 0]{*}");
        System.out.println(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp));
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithMilliseconds(baseTimestamp + 2000));
        rawQuery.setPageSize(1000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        QueryResult<TestTimeSeriesAPIEntity> result = storage.query(query, entityDefinition);
        Assert.assertNotNull(result);
    }

    @Test
    public void testReadByComplexQuery() throws QueryCompileException, IOException {
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[@cluster=\"c4ut\" AND @field4 > 1000 OR @datacenter =\"d4ut\" ]{@field1,@field2}");
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp + 2000));
        rawQuery.setPageSize(1000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        storage.query(query, entityDefinition);
    }

    @Test
    public void testReadByComplexQueryWithLike() throws QueryCompileException, IOException {
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[@cluster=\"c4ut\" AND @field4 > 1000 AND @field7 CONTAINS \"99404f47e309\" OR @datacenter =\"d4ut\" ]{@field1,@field2}");
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp + 2000));
        rawQuery.setPageSize(1000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        storage.query(query, entityDefinition);
    }

    @Test
    public void testWrite() throws IOException {
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<TestTimeSeriesAPIEntity>();
        int i = 0;
        while (i++ < 5) {
            TestTimeSeriesAPIEntity entity = newInstance();

            entityList.add(entity);
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() > 0);
    }

    @Test
    public void testWriteAndRead() throws IOException, QueryCompileException {
        // record insert init time
        final long startTime = System.currentTimeMillis();
        // Write 1000 entities
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<>();
        int i = 0;
        while (i++ < 5) {
            entityList.add(newInstance())/**/;
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() >= 5);
        // record insertion finish time
        long endTime = System.currentTimeMillis();

        // init read in time range [startTime, endTime)
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[]{*}");
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime + 1000));
        rawQuery.setPageSize(10000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        QueryResult queryResult = storage.query(query, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 5);
    }

    @Test
    public void testWriteAndDelete() throws IOException, QueryCompileException {
        // record insert init time
        final long startTime = System.currentTimeMillis();
        // Write 1000 entities
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<>();
        int i = 0;
        while (i++ < 5) {
            entityList.add(newInstance());
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() >= 5);
        // record insertion finish time
        long endTime = System.currentTimeMillis();

        // delete in time range [startTime, endTime)
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[]{*}");
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime - 1000));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime + 1000));
        rawQuery.setPageSize(1000000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        ModifyResult<String> queryResult = storage.delete(query, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 5);
    }

    @Test
    public void testWriteAndUpdate() throws IOException, QueryCompileException {
        // Write 5 entities
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<>();
        int i = 0;
        while (i++ < 5) {
            entityList.add(newInstance());
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() >= 5);

        // record insertion finish time
        ModifyResult<String> queryResult = storage.update(entityList, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 5);
    }

    @Test
    public void testWriteAndAggregation() throws IOException, QueryCompileException {
        // record insert init time
        final long startTime = System.currentTimeMillis();
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<>();
        int i = 0;
        while (i++ < 5) {
            entityList.add(newInstance());
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() >= 5);
        // record insertion finish time
        long endTime = System.currentTimeMillis();

        // init read in time range [startTime, endTime)
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[]<@cluster,@datacenter>{count,max(@field1),min(@field2),sum(@field3)}");
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime + 1000));
        rawQuery.setPageSize(1000000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        QueryResult queryResult = storage.query(query, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 1);
    }
}
