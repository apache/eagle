package org.apache.eagle.storage.operation;

import org.apache.eagle.storage.DataStorage;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * @Since 11/7/16.
 */
public class TestQueryStatement {

    private DataStorage mockDataStorage = mock(DataStorage.class);

    @Test
    public void testQueryExecute() throws Exception {
        RawQuery query = mock(RawQuery.class);
        CompiledQuery compiledQuery = mock(CompiledQuery.class);
        QueryStatement queryStatement = new QueryStatement(query);
        when(mockDataStorage.compile(query)).thenReturn(compiledQuery);
        when(compiledQuery.getServiceName()).thenReturn("TestTimeSeriesAPIEntity");
        queryStatement.execute(mockDataStorage);
        verify(mockDataStorage).query(any(), any());
    }
}
