package com.bigdatajumpstart.storm.oneusagov

import backtype.storm.tuple.Tuple
import com.bigdatajumpstart.storm.StreamingDataActionTestBase
import org.junit.Test

import static org.mockito.Mockito.mock
import static org.mockito.Mockito.when

class EnrichLinkBoltActionTest extends StreamingDataActionTestBase {

    @Test
    void testBoltAction() {

        OneUsaGovRequest req = new OneUsaGovRequest()
        req.longUrl = "http://bit.ly/17h37t8"

        // create a "mock" Tuple with Mockito
        Tuple mockTuple = mock(Tuple)
        when(mockTuple.getString(0)).thenReturn("foo")
        when(mockTuple.getValue(1)).thenReturn(req)

        //ConsoleBoltAction action = new ConsoleBoltAction()
        //action.embedlyService = mock(EmbedlyService)
        //action.execute(new OutputCollector(this), mockTuple)
    }
}
