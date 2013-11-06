package com.bigdatajumpstart.storm

import org.springframework.beans.factory.annotation.Autowired

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.SolrServer
import org.apache.solr.client.solrj.impl.BinaryResponseParser
import org.apache.solr.client.solrj.request.QueryRequest
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.SolrDocument

/**
 */
class SolrClient {

    @Autowired
    SolrServer solrServer

    SolrDocument get(String docId, String... fields) {
        SolrQuery q = new SolrQuery();
        q.setRequestHandler("/get");
        q.set("id", docId);
        q.setFields(fields);
        QueryRequest req = new QueryRequest(q)
        req.setResponseParser(new BinaryResponseParser())
        QueryResponse rsp = req.process(solrServer)
        return (SolrDocument)rsp.getResponse().get("doc")
    }
}
