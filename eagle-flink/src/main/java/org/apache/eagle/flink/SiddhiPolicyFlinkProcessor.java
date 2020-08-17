package org.apache.eagle.flink;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.List;
import java.util.Map;

/**
 * A Flink processor to evaluate Siddhi policy
 *
 * The steps to evaluate Siddhi policy should follow Flink's lifecycle methods,
 * Note: this processor must be serialiable as Flink runtime may persist it for failover
 * 1. setup phase. Initialize Siddhi runtime in open() method
 * 2. event process phase.
 *   In Siddhi stream callback, invoke Flink collector
 * 3. cleanup phase
 */
@RequiredArgsConstructor
@Slf4j
public class SiddhiPolicyFlinkProcessor extends KeyedProcessFunction<Long, StreamEvent, AlertStreamEvent> {
    private final Map<String, StreamDefinition> streamDefs;
    private final PolicyDefinition policyDefinition;

    private volatile SiddhiManager siddhiManager;
    private ExecutionPlanRuntime executionRuntime;
    /**
     * setup phase
     */
    @Override
    public void open(Configuration parameters) throws Exception{
        this.siddhiManager = new SiddhiManager();
        String plan = SiddhiDefinitionAdapter.buildSiddhiExecutionPlan(policyDefinition, streamDefs);
        log.info("Siddhi execution plan: {}", plan);
        try {
            this.executionRuntime = siddhiManager.createExecutionPlanRuntime(plan);
            log.info("Created siddhi runtime {}", executionRuntime.getName());
        } catch (Exception parserException) {
            log.error("Failed to create siddhi runtime for policy: {}, siddhi plan: \n\n{}\n",
                    this.policyDefinition.getName(), plan, parserException);
            throw parserException;
        }

        // fixme what to set up for PolicyHandlerContext
        PolicyHandlerContext context = new PolicyHandlerContext();

        // add output stream callback
        List<String> outputStreams = this.policyDefinition.getOutputStreams();
        for (final String outputStream : outputStreams) {
            if (executionRuntime.getStreamDefinitionMap().containsKey(outputStream)) {
                StreamDefinition streamDefinition = SiddhiDefinitionAdapter.convertFromSiddiDefinition(executionRuntime.getStreamDefinitionMap().get(outputStream));
                this.executionRuntime.addCallback(outputStream,
                        new AlertStreamCallback(outputStream, streamDefinition,
                                context, 0));
            } else {
                throw new IllegalStateException("Undefined output stream " + outputStream);
            }
        }
        this.executionRuntime.start();
    }

    /**
     * event process phase
     * input StreamEvent and output AlertStreamEvent
     * Note: in order for Siddhi runtime's callback can use @param out to collect output, we should add @param out
     * into @param value. In Siddhi callback, it can get original event so the collector.
     * In fact, the Object[] data's first element is reserved for this purpose.
     */
    @Override
    public void processElement(StreamEvent value, Context ctx, Collector<AlertStreamEvent> out) throws Exception {
        String streamId = value.getStreamId();
        InputHandler inputHandler = executionRuntime.getInputHandler(streamId);
        if (inputHandler != null) {
            value.attachFlinkCollector(out);
            inputHandler.send(value.getTimestamp(), value.getData());
            log.debug("sent event to siddhi stream {} ", streamId);
        } else {
            log.warn("No input handler found for stream {}", streamId);
        }
    }

    /**
     * cleanup phse
     */
    @Override
    public void close() throws Exception {
        this.executionRuntime.shutdown();
    }
}
