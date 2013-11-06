package com.bigdatajumpstart.storm.oneusagov

import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values

import backtype.storm.task.OutputCollector
import com.bigdatajumpstart.storm.StreamingDataAction
import com.codahale.metrics.Meter
import org.springframework.beans.factory.annotation.Autowired

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.SolrServer
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.SolrDocument
import org.apache.solr.common.SolrDocumentList

/**
 * Goes out to embed.ly service to get metadata for a link
 */
class EnrichLinkBoltAction implements StreamingDataAction {

    Cache<String,Embedly> enrichedLinks = null

    @Autowired
    SolrServer solrServer

    @Autowired
    EmbedlyService embedlyService

    // null pattern to avoid requesting metadata over and over
    private Embedly NULL_METADATA = new Embedly()

    private Meter solrQueries = MetricsSupport.meter(EnrichLinkBoltAction, "solrQueries")

    void execute(OutputCollector collector, Tuple input) {

        if (!enrichedLinks) {
            // setup an LRU cache to hold metadata that we've already pulled from embed.ly
            enrichedLinks = CacheBuilder.newBuilder().maximumSize(1000).build()
        }

        OneUsaGovRequest req = (OneUsaGovRequest)input.getValue(1)

        Embedly metadata = enrichedLinks.getIfPresent(req.longUrl)
        if (!metadata) {

            // next check Solr to see if we already know about this link
            String docId = input.getString(0)
            if (!existsInSolr(docId)) {
                try {
                    metadata = embedlyService.getLinkMetadata(req.longUrl)
                } catch (Exception exc) {
                    // todo: log it
                }
            }

            if (metadata) {
                enrichedLinks.put(req.longUrl, metadata)
            } else {
                enrichedLinks.put(req.longUrl, NULL_METADATA) // so we don't keep asking for it
            }
        }

        if (metadata && metadata.url) {
            collector.emit(new Values(input.getString(0), metadata))
        }
    }

    boolean existsInSolr(String docId) {
        solrQueries.mark()

        SolrQuery solrQuery = new SolrQuery("id:\"$docId\"")
        solrQuery.setFields("id")
        QueryResponse rsp = solrServer.query(solrQuery)
        SolrDocumentList hits = rsp.getResults()
        SolrDocument doc = (hits.getNumFound() > 0) ? hits.get(0) : null;
        return (doc != null)
    }
}
