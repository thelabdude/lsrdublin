package com.bigdatajumpstart.storm;

import org.apache.commons.cli.Option;
import backtype.storm.generated.StormTopology;

/**
 * A class that builds a StormTopology to be executed in a common framework by this driver.
 */
public interface StormTopologyFactory {
    Option[] getOptions();
    String getName();
    StormTopology build(StreamingApp app) throws Exception;
}
