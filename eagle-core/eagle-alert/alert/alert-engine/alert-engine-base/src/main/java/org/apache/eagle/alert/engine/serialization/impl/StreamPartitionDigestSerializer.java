package org.apache.eagle.alert.engine.serialization.impl;

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.serialization.Serializer;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Don't serialize streamId
 *
 * @see StreamPartition
 */
public class StreamPartitionDigestSerializer implements Serializer<StreamPartition> {
    public final static StreamPartitionDigestSerializer INSTANCE = new StreamPartitionDigestSerializer();

    private final Map<DigestBytes,StreamPartition> checkSumPartitionMap = new HashMap<>();
    private final Map<StreamPartition,DigestBytes> partitionCheckSumMap = new HashMap<>();

    @Override
    public void serialize(StreamPartition partition, DataOutput dataOutput) throws IOException {
        DigestBytes checkSum = partitionCheckSumMap.get(partition);
        if(checkSum == null){
            try {
                checkSum = digestCheckSum(partition);
                partitionCheckSumMap.put(partition,checkSum);
                checkSumPartitionMap.put(checkSum,partition);
            } catch (NoSuchAlgorithmException e) {
                throw new IOException(e);
            }
        }
        dataOutput.writeInt(checkSum.size());
        dataOutput.write(checkSum.toByteArray());
    }

    @Override
    public StreamPartition deserialize(DataInput dataInput) throws IOException {
        int checkSumLen = dataInput.readInt();
        byte[] checksum = new byte[checkSumLen];
        dataInput.readFully(checksum);
        StreamPartition partition = checkSumPartitionMap.get(new DigestBytes(checksum));
        if(partition == null){
            throw new IOException("Illegal partition checksum: "+checksum);
        }
        return partition;
    }

    private class DigestBytes {
        private final byte[] data;

        public DigestBytes(byte[] bytes){
            this.data = bytes;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof DigestBytes && Arrays.equals(data, ((DigestBytes) other).data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
        public int size(){
            return data.length;
        }
        public byte[] toByteArray(){
            return data;
        }
    }

    private DigestBytes digestCheckSum(Object obj) throws IOException, NoSuchAlgorithmException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        MessageDigest m = MessageDigest.getInstance("SHA1");
        m.update(baos.toByteArray());
        return new DigestBytes(m.digest());
    }
}