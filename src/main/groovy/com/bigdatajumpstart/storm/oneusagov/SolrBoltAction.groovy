package com.bigdatajumpstart.storm.oneusagov

import com.bigdatajumpstart.storm.StreamingDataAction
import com.codahale.metrics.Meter

import java.text.SimpleDateFormat

import backtype.storm.tuple.Tuple
import backtype.storm.task.OutputCollector

import storm.starter.util.TupleHelpers

import org.apache.solr.client.solrj.SolrServer
import org.apache.solr.common.SolrInputDocument

import org.springframework.beans.factory.annotation.Autowired

/**
 * Index link metadata in Solr.
 */
class SolrBoltAction implements StreamingDataAction {

    private final SimpleDateFormat isoDateFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

    @Autowired
    SolrServer solrServer

    private Meter linksIndexed = MetricsSupport.meter(SolrBoltAction, "linksIndexed")

    SolrBoltAction() {
        isoDateFmt.setTimeZone(TimeZone.getTimeZone("UTC"))
    }

    void execute(OutputCollector collector, Tuple input) {

        if (TupleHelpers.isTickTuple(input)) {
            // sends a soft commit to Solr every N seconds
            solrServer.commit(false,false,true)
            return;
        }

        String docId = input.getString(0)
        Embedly meta = (Embedly)input.getValue(1)
        if (!meta || !meta.url)
            return; // nothing to index

        SolrInputDocument doc = new SolrInputDocument()
        doc.addField("id", docId)
        doc.addField("url_s", meta.url)
        doc.addField("type_s", meta.type)
        doc.addField("indexed_on_tdt", isoDateFmt.format(new Date()))
        addOptionalField(doc, "title_en", meta.title)
        addOptionalField(doc, "description_en", meta.description)
        addOptionalField(doc, "html_s", meta.html)
        addOptionalField(doc, "image_s", meta.thumbnailUrl)
        addOptionalField(doc, "author_name_en", meta.authorName)
        addOptionalField(doc, "author_url_s", meta.authorUrl)
        addOptionalField(doc, "provider_name_s", meta.providerName)
        addOptionalField(doc, "provider_url_s", meta.providerUrl)

        // send to Solr
        solrServer.add(doc)

        linksIndexed.mark()
    }

    def addOptionalField(SolrInputDocument doc, String fieldName, String value) {
        if (value)
            doc.addField(fieldName, value)
    }
}
