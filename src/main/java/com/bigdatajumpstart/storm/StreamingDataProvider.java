package com.bigdatajumpstart.storm;

import java.util.Map;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Interface to a Spring-managed POJO that performs some spout logic in Storm topology.
 */
public interface StreamingDataProvider {
    void open(Map stormConf);
    boolean next(NamedValues record) throws Exception;
}
