package org.apache.eagle.common.agg;

/**
 * Since 8/3/16.
 */
public class TimeBatchAggSpec {
    long batchSize;
    long offset;
    Groupby groupby;
    Agg agg;
}
