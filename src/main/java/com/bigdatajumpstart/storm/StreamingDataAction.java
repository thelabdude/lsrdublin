package com.bigdatajumpstart.storm;

import backtype.storm.tuple.Tuple;

import backtype.storm.task.OutputCollector;

/**
 * Interface to a POJO that implements some action on streaming data in a Storm topology.
 */
public interface StreamingDataAction {
    void execute(OutputCollector collector, Tuple input);
}