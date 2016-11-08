package org.apache.eagle.storage.operation;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.storage.DataStorage;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.mockito.Mockito.*;

/**
 * @Since 11/7/16.
 */
public class TestDeleteStatement {

    private DataStorage mockDataStorage = mock(DataStorage.class);

    @Test
    public void testResultNotNullIds() throws IOException {
        DeleteStatement deleteStatement = new DeleteStatement("TestTimeSeriesAPIEntity");
        deleteStatement.setIds(new ArrayList<>());
        deleteStatement.execute(mockDataStorage);
        verify(mockDataStorage).deleteByID(anyList(), any(EntityDefinition.class));
    }

    @Test
    public void testResultNotNullQuery() throws Exception {
        RawQuery query = mock(RawQuery.class);
        CompiledQuery compiledQuery = mock(CompiledQuery.class);
        DeleteStatement deleteStatement = new DeleteStatement(query);
        when(mockDataStorage.compile(query)).thenReturn(compiledQuery);
        when(compiledQuery.getServiceName()).thenReturn("TestTimeSeriesAPIEntity");
        deleteStatement.execute(mockDataStorage);
        verify(mockDataStorage).delete(any(CompiledQuery.class), any(EntityDefinition.class));
    }

    @Test
    public void testResultNotNullEntities() throws IOException {
        DeleteStatement deleteStatement = new DeleteStatement(TestTimeSeriesAPIEntity.class.getSimpleName());
        deleteStatement.setEntities(new ArrayList<>());
        deleteStatement.execute(mockDataStorage);
        verify(mockDataStorage).delete(anyList(), any(EntityDefinition.class));
    }

    @Test(expected = IOException.class)
    public void testResultAllNull() throws IOException {
        DeleteStatement deleteStatement = new DeleteStatement(TestTimeSeriesAPIEntity.class);
        deleteStatement.execute(mockDataStorage);
    }
}
