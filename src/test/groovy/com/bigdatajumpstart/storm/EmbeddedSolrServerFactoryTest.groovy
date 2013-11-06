package com.bigdatajumpstart.storm

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.BinaryResponseParser
import org.apache.solr.client.solrj.request.QueryRequest
import org.apache.solr.common.SolrInputDocument

import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.SolrDocument

import static org.junit.Assert.assertNotNull
import org.junit.Test

import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer

class EmbeddedSolrServerFactoryTest {

    @Test
    void testBootstrapEmbeddedSolrServer() {

        File tmp = new File("./target/tmp")
        tmp.mkdirs()

        EmbeddedSolrServerFactory factory = new EmbeddedSolrServerFactory()
        factory.setPercolatorZipResource("percolator.zip")
        factory.setEmbeddedSolrRootPath(tmp.getAbsolutePath())

        EmbeddedSolrServer solrServer = factory.getEmbeddedSolrServer("embedded://embedded")
        assertNotNull(solrServer)

        // index a doc
        SolrInputDocument doc = new SolrInputDocument()
        doc.addField("id", "1")
        doc.addField("text", "foo")
        solrServer.add(doc)

        // soft commit
        solrServer.commit(true,true,true);

        // find it
        SolrQuery q = new SolrQuery();
        q.setRequestHandler("/get");
        q.set("id", "1");
        QueryRequest req = new QueryRequest(q)
        req.setResponseParser(new BinaryResponseParser())
        QueryResponse rsp = req.process(solrServer)
        SolrDocument foundIt = (SolrDocument)rsp.getResponse().get("doc")
        assertNotNull(foundIt)
    }
}
