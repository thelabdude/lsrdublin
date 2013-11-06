package com.bigdatajumpstart.storm.oneusagov

import com.bigdatajumpstart.storm.MessageHandler
import com.bigdatajumpstart.storm.MessageStream
import com.bigdatajumpstart.storm.NamedValues
import com.bigdatajumpstart.storm.StreamingDataProvider

import java.util.concurrent.LinkedBlockingQueue
import com.fasterxml.jackson.databind.ObjectMapper

/**
 *  Receives messages from the 1.usa.gov stream and emits them to Storm.
 */
class OneUsaGovStreamingDataProvider implements StreamingDataProvider, MessageHandler {

    MessageStream messageStream

    private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(1000)
    private ObjectMapper objectMapper = new ObjectMapper()

    void open(Map stormConf) {
        messageStream.receive(this)
    }

    boolean next(NamedValues nv) {
        String msg = queue.poll()
        if (msg) {
            OneUsaGovRequest req = objectMapper.readValue(msg, OneUsaGovRequest)
            if (req != null && req.globalBitlyHash != null) {
                nv.set(OneUsaGovTopology.GLOBAL_BITLY_HASH, req.globalBitlyHash)
                nv.set(OneUsaGovTopology.JSON_PAYLOAD, req)
                return true
            }
        }

        return false
    }

    void handleMessage(String msg) {
        queue.offer(msg);
    }
}
