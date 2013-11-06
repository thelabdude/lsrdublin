package com.bigdatajumpstart.storm

import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream

import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer
import org.apache.solr.core.CoreContainer
import org.apache.solr.core.SolrResourceLoader

import org.apache.log4j.Logger

/**
 * Supports one or more embedded Solr servers in the same JVM
 */
class EmbeddedSolrServerFactory {

    static final Logger log = Logger.getLogger(EmbeddedSolrServerFactory)

    String embeddedSolrRootPath
    String percolatorZipResource

    Map<String,EmbeddedSolrServer> servers = new HashMap<String,EmbeddedSolrServer>()

    /*
    key to this impl is a minimal set of config loaded in memory, no
    */
    synchronized EmbeddedSolrServer getEmbeddedSolrServer(String embeddedUrl) {

        EmbeddedSolrServer solr = servers.get(embeddedUrl)
        if (!solr) {
            solr = bootstrapEmbeddedSolrServer(embeddedUrl)
            servers.put(embeddedUrl, solr)
        }
        return solr
    }

    private EmbeddedSolrServer bootstrapEmbeddedSolrServer(String embeddedUrl) {
        File rootDir = new File(embeddedSolrRootPath);

        log.info(String.format("Attempting to bootstrap EmbeddedSolrServer instance %s starting from PWD: %s",
                embeddedUrl, rootDir.getAbsolutePath()));

        String solrHomeKey = embeddedUrl.substring("embedded://".length()).replaceAll("/", "_");
        String solrHomeName = solrHomeKey + "_solr_home";

        File instanceDir = new File(rootDir, solrHomeName+"/collection1");
        if (instanceDir.isDirectory()) rmdir(instanceDir);
        instanceDir.mkdirs();

        log.info(String.format("Created new Solr core instance directory: %s", instanceDir.getAbsolutePath()));

        // Locate the percolator.zip resource on the classpath ... extract rootDir
        ZipInputStream zipIn = null
        ZipEntry ze = null
        try {
            zipIn = new ZipInputStream(getResourceAsStream(percolatorZipResource))
            while ((ze = zipIn.getNextEntry()) != null) {
                File newFile = new File(instanceDir, ze.getName());
                if (ze.isDirectory()) {
                    newFile.mkdirs()
                } else {
                    System.out.println("Unzipping entry "+ze.getName()+" to "+newFile.getAbsolutePath());
                    deflate(zipIn, newFile)
                }
            }
        } finally {
            if (zipIn != null) {
                try { zipIn.close() } catch (Exception ignore){}
            }
        }

        SolrResourceLoader solrResourceLoader = new SolrResourceLoader(instanceDir.getParentFile().getAbsolutePath());
        CoreContainer coreContainer = new CoreContainer(solrResourceLoader);
        coreContainer.load();
        EmbeddedSolrServer ess = new EmbeddedSolrServer(coreContainer, "collection1");
        return ess;
    }

    private InputStream getResourceAsStream(String pathOnCpath) throws Exception {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(pathOnCpath);
        if (inputStream == null) {
            throw new IOException("Required resource '"+pathOnCpath+
                    "' not found on classpath ... cannot bootstrap EmbeddedSolrServer!");
        }
        return inputStream
    }

    private void rmdir(File f) throws IOException {
        if (f.isDirectory()) {
            for (File child : f.listFiles())
                rmdir(child)
            f.delete()
        } else if (f.isFile()) {
            f.delete()
        }
    }

    private void deflate(ZipInputStream zipIn, File dest) {
        File parentFile = dest.getParentFile()
        if (!parentFile.isDirectory()) {
            parentFile.mkdirs()
            System.out.println("created directory "+parentFile.getAbsolutePath());
        }

        byte[] buf = new byte[1024]
        int r = 0
        FileOutputStream fos = null
        try {
            fos = new FileOutputStream(dest, false)
            while ((r = zipIn.read(buf)) != -1)
                fos.write(buf, 0, r)
            fos.flush()
        } finally {
            if (zipIn != null) {
                try {
                    zipIn.closeEntry()
                } catch (Exception ignoreMe) {}
            }
            if (fos != null) {
                try {
                    fos.close()
                } catch (Exception ignoreMe) {}
            }
        }
    }
}
