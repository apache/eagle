package org.apache.eagle.alert.engine.evaluator.impl;


import org.apache.eagle.alert.engine.AlertStreamCollector;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class AlertBoltOutputCollectorSparkWrapper implements AlertStreamCollector {
    private final LinkedList<Tuple2<String, AlertStreamEvent>> collector = new LinkedList<Tuple2<String, AlertStreamEvent>>();

    public AlertBoltOutputCollectorSparkWrapper() {
    }

    @Override
    public void emit(AlertStreamEvent event) {
        collector.add(new Tuple2(event.getStreamId(), event));
    }

    public List<Tuple2<String, AlertStreamEvent>> emitResult() {
        if (collector.isEmpty()) {
            return Collections.emptyList();
        }
        LinkedList<Tuple2<String, AlertStreamEvent>> result = new LinkedList<Tuple2<String, AlertStreamEvent>>();
        result.addAll(collector);
        return result;
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {

    }
}
