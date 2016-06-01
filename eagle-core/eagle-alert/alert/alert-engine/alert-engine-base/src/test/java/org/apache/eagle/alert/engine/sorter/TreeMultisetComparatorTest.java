package org.apache.eagle.alert.engine.sorter;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.sorter.impl.PartitionedEventTimeOrderingComparator;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.TreeMultiset;

/**
 * Since 5/10/16.
 */
public class TreeMultisetComparatorTest {
    /**
     * if 2 different events have the same timestamp, and comparator return 0 when timestamp is same,
     * when they are added into TreeMultiset, the second event will be replaced by the first event
     */
    @Test
    public void testComparator(){
        TreeMultiset<PartitionedEvent> set = TreeMultiset.create(PartitionedEventTimeOrderingComparator.INSTANCE);

        // construct PartitionEvent1
        PartitionedEvent event1 = new PartitionedEvent();
        StreamPartition sp = new StreamPartition();
        sp.setColumns(Arrays.asList("host"));
        sp.setSortSpec(null);
        sp.setStreamId("testStreamId");
        sp.setType(StreamPartition.Type.GROUPBY);
        event1.setPartition(sp);
        event1.setPartitionKey(1000);
        StreamEvent e1 = new StreamEvent();
        e1.setData(new Object[]{18.4});
        e1.setStreamId("testStreamId");
        e1.setTimestamp(1462909984000L);
        event1.setEvent(e1);

        // construct PartitionEvent2 with same timestamp but different value
        PartitionedEvent event2 = new PartitionedEvent();
        event2.setPartition(sp);
        event2.setPartitionKey(1000);
        StreamEvent e2 = new StreamEvent();
        e2.setData(new Object[]{16.3});
        e2.setStreamId("testStreamId");
        e2.setTimestamp(1462909984000L);
        event2.setEvent(e2);

        // construct PartitionEvent2 with same timestamp but different value
        PartitionedEvent event3 = new PartitionedEvent();
        event3.setPartition(sp);
        event3.setPartitionKey(1000);
        StreamEvent e3 = new StreamEvent();
        e3.setData(new Object[]{14.3});
        e3.setStreamId("testStreamId");
        e3.setTimestamp(1462909984001L);
        event3.setEvent(e3);

        PartitionedEvent event4 = new PartitionedEvent();
        event4.setPartition(sp);
        event4.setPartitionKey(1000);
        StreamEvent e4 = new StreamEvent();
        e4.setData(new Object[]{14.3});
        e4.setStreamId("testStreamId");
        e4.setTimestamp(1462909984001L);
        event4.setEvent(e4);

        Assert.assertNotEquals(event2,event3);
        Assert.assertEquals(event3,event4);

        // check content in set
        set.add(event1);
        set.add(event2);
        set.add(event3);
        set.add(event4);
        Assert.assertEquals(4, set.size());
        set.forEach(System.out::println);


        Assert.assertEquals(-1,PartitionedEventTimeOrderingComparator.INSTANCE.compare(event1,event2));
        Assert.assertEquals(-1,PartitionedEventTimeOrderingComparator.INSTANCE.compare(event1,event3));
        Assert.assertEquals(-1,PartitionedEventTimeOrderingComparator.INSTANCE.compare(event2,event3));
        Assert.assertEquals(0,PartitionedEventTimeOrderingComparator.INSTANCE.compare(event3,event4));

        Iterator<PartitionedEvent> it = set.iterator();
        Assert.assertEquals(16.3,it.next().getData()[0]);
        Assert.assertEquals(18.4,it.next().getData()[0]);
        Assert.assertEquals(14.3,it.next().getData()[0]);
        Assert.assertEquals(14.3,it.next().getData()[0]);
    }
}
