package com.bigdatajumpstart.storm;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

/**
 * Executes a Spring-managed BoltAction implementation.
 */
public class SpringBolt extends BaseRichBolt {

    private static final Logger log = Logger.getLogger(SpringBolt.class);

    protected String boltLogicBeanId;
    protected Fields outputFields;
    protected int tickRate = -1;

    private transient StreamingDataAction delegate;
    private transient OutputCollector collector;
    private transient Map stormConf;

    public SpringBolt(String boltBeanId) {
        this(boltBeanId, null);
    }

    public SpringBolt(String boltBeanId, Fields outputFields) {
        this.boltLogicBeanId = boltBeanId;
        this.outputFields = outputFields;
    }

    public void setTickRate(int rateInSecs) {
        this.tickRate = rateInSecs;
    }

    public int getTickRate() {
        return tickRate;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.stormConf = map;
        this.collector = outputCollector;
        getStreamingDataActionBean();
    }

    @Override
    public void execute(Tuple input) {
        //def execContext = execTimer.time()
        try {
            // tracking which bolts touch the signal for debugging
            delegate.execute(collector, input);
            collector.ack(input);
        } catch (Throwable exc) {
            exc.printStackTrace();

            collector.reportError(exc);
            //log.error("Failed to process Tuple ["+input+"] due to: "+exc, exc)
            collector.fail(input);
        } finally {
            //execContext.stop()
        }
    }

    @Override
    public void cleanup() {

        super.cleanup();

        //log.debug("Shutdown ${getClass().getName()}")
    }

    protected StreamingDataAction getStreamingDataActionBean() {
        if (delegate == null) {
            // Get the Bolt Logic POJO from Spring
            delegate = (StreamingDataAction)StreamingApp.spring((String)stormConf.get("storm.id")).getBean(boltLogicBeanId);
        }
        return delegate;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        if (outputFields != null && outputFields.size() > 0) {
            outputFieldsDeclarer.declare(outputFields);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        if (tickRate > 0) {
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickRate);
        }
        return conf;
    }
}
