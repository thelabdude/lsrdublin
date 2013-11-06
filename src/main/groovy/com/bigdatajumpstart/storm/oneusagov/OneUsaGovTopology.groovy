package com.bigdatajumpstart.storm.oneusagov

import backtype.storm.topology.TopologyBuilder
import backtype.storm.generated.StormTopology
import backtype.storm.tuple.Fields
import com.bigdatajumpstart.storm.StormTopologyFactory
import com.bigdatajumpstart.storm.StreamingApp
import com.bigdatajumpstart.storm.SpringBolt
import com.bigdatajumpstart.storm.SpringSpout
import org.apache.commons.cli.Option

import storm.starter.bolt.IntermediateRankingsBolt
import storm.starter.bolt.RollingCountBolt
import storm.starter.bolt.TotalRankingsBolt

/**
 * Constructs a StormTopology for processing events from the 1.usa.gov stream.
 */
class OneUsaGovTopology implements StormTopologyFactory {

    static final String GLOBAL_BITLY_HASH = "globalBitlyHash"
    static final String JSON_PAYLOAD = "jsonPayload"
    static final String EMBEDLY = "embedly"
    static final Fields globalBitlyHashGrouping = new Fields(GLOBAL_BITLY_HASH)
    static final Fields spoutFields = new Fields(GLOBAL_BITLY_HASH, JSON_PAYLOAD)

    static final Fields enrichedLinkFields = new Fields(GLOBAL_BITLY_HASH, EMBEDLY)

    @Override
    Option[] getOptions() {
        return new Option[0]
    }

    @Override
    String getName() {
        return getClass().getSimpleName()
    }

    @Override
    StormTopology build(StreamingApp app) throws Exception {
        // todo: make these configurable
        int windowSizeInSeconds = 120;
        int topN = 20;
        int emitFrequencyInSeconds = 30;

        TopologyBuilder builder = new TopologyBuilder()

        builder.setSpout("1.usa.gov-spout", new SpringSpout("oneUsaGovStreamingDataProvider", spoutFields), 1)

        builder.setBolt("enrich-link-bolt", new SpringBolt("enrichLinkAction", enrichedLinkFields), 3)
               .fieldsGrouping("1.usa.gov-spout", globalBitlyHashGrouping)

        SpringBolt solrBolt = new SpringBolt("solrBoltAction")
        solrBolt.setTickRate(30) // have storm send a tick tuple every 30 secs
        builder.setBolt("index-link-bolt", solrBolt, 1).globalGrouping("enrich-link-bolt");

        builder.setBolt("rolling-count-bolt", new RollingCountBolt(windowSizeInSeconds, emitFrequencyInSeconds), 1)
               .fieldsGrouping("1.usa.gov-spout", globalBitlyHashGrouping);

        builder.setBolt("intermediate-ranker-bolt", new IntermediateRankingsBolt(topN, emitFrequencyInSeconds), 1)
               .fieldsGrouping("rolling-count-bolt", new Fields("obj"));

        builder.setBolt("total-ranker-bolt", new TotalRankingsBolt(topN, emitFrequencyInSeconds))
               .globalGrouping("intermediate-ranker-bolt");

        builder.setBolt("persist-rankings-bolt", new SpringBolt("persistRankingsBoltAction", null), 1)
               .globalGrouping("total-ranker-bolt")

        return builder.createTopology()
    }
}
