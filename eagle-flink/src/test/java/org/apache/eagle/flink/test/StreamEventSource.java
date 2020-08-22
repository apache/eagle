package org.apache.eagle.flink.test;

import org.apache.eagle.flink.StreamEvent;
import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A stream of transactions.
 */
@Public
public class StreamEventSource extends FromIteratorFunction<StreamEvent> {

    private static final long serialVersionUID = 1L;

    public StreamEventSource(boolean unbounded) {
        super(new RateLimitedIterator<>(unbounded ? StreamEventIterator.unbounded() : StreamEventIterator.bounded()));
    }

    public StreamEventSource() {
        this(true);
    }

    private static class RateLimitedIterator<T> implements Iterator<T>, Serializable {

        private static final long serialVersionUID = 1L;

        private final Iterator<T> inner;

        private RateLimitedIterator(Iterator<T> inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public T next() {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return inner.next();
        }
    }
}
