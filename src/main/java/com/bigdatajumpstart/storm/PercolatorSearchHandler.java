package com.bigdatajumpstart.storm;

import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.HashMap;

import org.apache.solr.core.PluginInfo;

import org.apache.lucene.search.Query;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

public class PercolatorSearchHandler extends SearchHandler implements Closeable {

    //private static final Logger log = Logger.getLogger(NDCLoggingSearchHandler.class);
    //private static final AtomicInteger ndc = new AtomicInteger(0);

    private Map<String,String> queryMap = new HashMap<String,String>();

    public void init(PluginInfo info) {
        super.init(info);

        // get from some "shared" location or database
        BufferedReader br = null;
        try {
            InputStream in = this.getClass().getClassLoader().getResourceAsStream("percolator_queries.txt");
            InputStreamReader isr = new InputStreamReader(in, "UTF-8");
            br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.length() == 0 || line.startsWith("#")) continue;

                int eq = line.indexOf("=");
                if (eq == -1) continue;
                String sig = line.substring(0,eq).trim();
                String query = line.substring(eq+1);

               // String defType = params.get(QueryParsing.DEFTYPE, QParserPlugin.DEFAULT_QTYPE);

                queryMap.put(sig, query);
            }
        } catch (Exception exc) {
            if (exc instanceof RuntimeException) {
                throw (RuntimeException)exc;
            } else {
                throw new RuntimeException(exc);
            }
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception ignore){}
            }
        }
    }

    public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
        SolrRequestInfo.getRequestInfo().addCloseHook(this);
        //NDC.push("Q:" + ndc.incrementAndGet());
        log.info(req.getParamString());

        // get the doc ID from the request ...

        // do once: parse the queries into Lucene queries

        // iterate over all queries, keeping track of the matches

        // return a list of matching queries

        super.handleRequest(req, rsp);
    }

    public void close() throws IOException {
        //NDC.remove();
    }
}
