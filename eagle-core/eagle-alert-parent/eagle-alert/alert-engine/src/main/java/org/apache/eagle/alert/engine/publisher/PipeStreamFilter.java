package org.apache.eagle.alert.engine.publisher;

import org.apache.eagle.alert.engine.model.AlertStreamEvent;

import java.util.ArrayList;
import java.util.List;

public class PipeStreamFilter implements AlertStreamFilter {

    private final List<AlertStreamFilter> filters;

    public PipeStreamFilter(AlertStreamFilter... filters) {
        this.filters = new ArrayList<>();
        for (AlertStreamFilter filter : filters) {
            this.filters.add(filter);
        }
    }

    @Override
    public AlertStreamEvent filter(AlertStreamEvent event) {
        AlertStreamEvent current = event;
        for (AlertStreamFilter filter : this.filters) {
            if (current == null) {
                return null;
            }
            current = filter.filter(current);
        }
        return current;
    }
}