package com.bigdatajumpstart.storm

import backtype.storm.Config
import backtype.storm.task.IOutputCollector
import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.IBasicBolt
import backtype.storm.tuple.Tuple

import org.junit.After
import org.junit.Before

import static org.junit.Assert.assertNotNull
import static org.junit.Assert.fail
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.when

class StreamingDataActionTestBase implements IOutputCollector {

    def emitted = [:]

    Config stormConf

    @Before
    void setup() {
        stormConf = StreamingApp.getConfig(StreamingApp.ENV.test.toString())
        stormConf.put("storm.id", "unit-test")
    }

    @After
    void tearDown() {

    }

    List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        def key = streamId?:"default"
        def tuplesInStream = emitted.get(key)
        if (!tuplesInStream) {
            tuplesInStream = []
            emitted[key] = tuplesInStream
        }
        tuplesInStream << tuple
    }

    void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        // todo
    }

    @Override
    void ack(Tuple tuple) {
    }

    @Override
    void fail(Tuple tuple) {
    }

    @Override
    void reportError(Throwable throwable) {
    }

}
