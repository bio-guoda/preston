package org.globalbioticinteractions.preston.process;

import org.apache.commons.rdf.api.IRI;
import org.globalbioticinteractions.preston.cmd.CrawlContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public abstract class ProcessorReadOnly extends StatementProcessor {

    private final BlobStoreReadOnly blobStoreReadOnly;
    private final CrawlContext crawlContext;

    public ProcessorReadOnly(BlobStoreReadOnly blobStoreReadOnly, CrawlContext crawlContext, StatementListener... listeners) {
        super(listeners);
        Objects.requireNonNull(blobStoreReadOnly);
        this.blobStoreReadOnly = blobStoreReadOnly;
        Objects.requireNonNull(crawlContext);
        this.crawlContext = crawlContext;
    }

    protected InputStream get(IRI uri) throws IOException {
        return blobStoreReadOnly.get(uri);
    }

    protected CrawlContext getCrawlContext() {
        return this.getCrawlContext();
    }


}
