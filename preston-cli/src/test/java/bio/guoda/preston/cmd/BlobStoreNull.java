package bio.guoda.preston.cmd;

import bio.guoda.preston.store.BlobStore;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

public class BlobStoreNull implements BlobStore {

    public final AtomicInteger putAttemptCount = new AtomicInteger(0);
    public final AtomicInteger getAttemptCount = new AtomicInteger(0);
    public ByteArrayOutputStream mostRecentBlob;


    @Override
    public IRI put(InputStream is) throws IOException {
        mostRecentBlob = new ByteArrayOutputStream();
        IOUtils.copy(is, mostRecentBlob);
        putAttemptCount.incrementAndGet();
        return null;
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        getAttemptCount.incrementAndGet();
        return null;
    }
}
