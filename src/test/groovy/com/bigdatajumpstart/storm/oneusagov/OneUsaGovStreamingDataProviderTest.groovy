package com.bigdatajumpstart.storm.oneusagov

import com.bigdatajumpstart.storm.MessageStream
import com.bigdatajumpstart.storm.NamedValues
import com.bigdatajumpstart.storm.StreamingDataProviderTestBase

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.junit.Assert.assertNotNull
import static org.mockito.Mockito.mock

import org.junit.Test

/**
 * Verifies the spout dataProvider class produces the correct output fields.
 */
class OneUsaGovStreamingDataProviderTest extends StreamingDataProviderTestBase {

    @Test
    void testDataProvider() {

        String jsonStr = '''{
            "a": "user-agent", "c": "US",
            "nk": 0, "tz": "America/Los_Angeles",
            "gr": "OR", "g": "2BktiW",
            "h": "12Me4B2", "l": "usairforce",
            "al": "en-us", "hh": "1.usa.gov",
            "r": "http://example.com/foo",
            "u": "http://example.com/bar",
            "t": 1378823179, "hc": 1365693094,
            "cy": "Portland", "ll": [ 45.523399, -122.676201 ]
        }'''

        OneUsaGovStreamingDataProvider dataProvider = new OneUsaGovStreamingDataProvider()
        dataProvider.setMessageStream(mock(MessageStream))
        dataProvider.open(stormConf) // Config setup in base class
        dataProvider.handleMessage(jsonStr)

        NamedValues record = new NamedValues(OneUsaGovTopology.spoutFields)
        assertTrue dataProvider.next(record)
        assertTrue record.getMessageId() == null
        List<Object> values = record.values()
        assertNotNull values
        assertTrue values.size() == OneUsaGovTopology.spoutFields.size()
        assertEquals "2BktiW", values.get(0)
        assertTrue (values.get(1) instanceof OneUsaGovRequest)
    }
} 
