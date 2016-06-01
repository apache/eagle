package org.apache.eagle.alert.engine.serialization.impl;

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.apache.eagle.alert.engine.serialization.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Don't serialize streamId
 *
 * @see StreamPartition
 */
public class StreamPartitionSerializer implements Serializer<StreamPartition> {
    public final static StreamPartitionSerializer INSTANCE = new StreamPartitionSerializer();

    @Override
    public void serialize(StreamPartition partition, DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(partition.getType().toString());
        if(partition.getColumns() == null || partition.getColumns().size() == 0){
            dataOutput.writeInt(0);
        } else {
            dataOutput.writeInt(partition.getColumns().size());
            for(String column:partition.getColumns()){
                dataOutput.writeUTF(column);
            }
        }
        if(partition.getSortSpec() == null){
            dataOutput.writeByte(0);
        }else {
            dataOutput.writeByte(1);
            dataOutput.writeUTF(partition.getSortSpec().getWindowPeriod());
            dataOutput.writeInt(partition.getSortSpec().getWindowMargin());
        }
    }

    @Override
    public StreamPartition deserialize(DataInput dataInput) throws IOException {
        StreamPartition partition = new StreamPartition();
        partition.setType(StreamPartition.Type.locate(dataInput.readUTF()));
        int colSize = dataInput.readInt();
        if(colSize>0){
            List<String> columns = new ArrayList<>(colSize);
            for(int i=0;i<colSize;i++){
                columns.add(dataInput.readUTF());
            }
            partition.setColumns(columns);
        }
        if(dataInput.readByte() == 1){
            String period = dataInput.readUTF();
            int margin = dataInput.readInt();

            StreamSortSpec sortSpec = new StreamSortSpec();
            sortSpec.setWindowPeriod(period);
            sortSpec.setWindowMargin(margin);
            partition.setSortSpec(sortSpec);
        }
        return partition;
    }
}