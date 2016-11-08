package org.apache.eagle.storage.operation;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.storage.DataStorage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @Since 11/7/16.
 */
public class TestRowkeyQueryStatement {
    private DataStorage mockDataStorage = mock(DataStorage.class);

    @Test
    public void testRowkeyExecute() throws Exception {
        String rowkey = "rowkey";
        List<String> rowkeys = new ArrayList<>();
        rowkeys.add(rowkey);
        EntityDefinition entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class);
        RowkeyQueryStatement rowkeyQueryStatement1 = new RowkeyQueryStatement(rowkey, entityDefinition);
        RowkeyQueryStatement rowkeyQueryStatement2 = new RowkeyQueryStatement(rowkey, TestTimeSeriesAPIEntity.class.getSimpleName());
        RowkeyQueryStatement rowkeyQueryStatement3 = new RowkeyQueryStatement(rowkeys, entityDefinition);
        RowkeyQueryStatement rowkeyQueryStatement4 = new RowkeyQueryStatement(rowkeys, TestTimeSeriesAPIEntity.class.getSimpleName());
        rowkeyQueryStatement1.execute(mockDataStorage);
        rowkeyQueryStatement2.execute(mockDataStorage);
        rowkeyQueryStatement3.execute(mockDataStorage);
        rowkeyQueryStatement4.execute(mockDataStorage);
        verify(mockDataStorage, times(4)).queryById(any(), any());
    }
}
