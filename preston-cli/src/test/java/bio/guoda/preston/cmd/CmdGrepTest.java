package bio.guoda.preston.cmd;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class CmdGrepTest {

    public static final String DWCA = "/bio/guoda/preston/dwca-20180905.zip";

    @Test
    public void processOneVersion() {
        IRI aContentHash = toIRI("hash://sha256/blabla");

        BlobStoreNull blobStoreNull = new BlobStoreNull(){
            @Override
            public InputStream get(IRI key) throws IOException {
                if (getAttemptCount.incrementAndGet() > 2 || !aContentHash.equals(key)) {
                    throw new IOException("kaboom!");
                }
                return getClass().getResourceAsStream(DWCA);
            }
        };

        CmdGrep cmdGrep = new CmdGrep();

        Quad quad = toStatement(toIRI("something"), HAS_VERSION, aContentHash);
        cmdGrep.setInputStream(new ByteArrayInputStream(quad.toString().getBytes()));

        cmdGrep.run(blobStoreNull);

        assertThat(blobStoreNull.getAttemptCount.get(), is(1));
    }

    @Test(expected = RuntimeException.class)
    public void attemptToProcessUnvailableHash() {
        IRI aContentHash = toIRI("hash://sha256/blabla");

        BlobStoreNull blobStoreNull = new BlobStoreNull(){
            @Override
            public InputStream get(IRI key) throws IOException {
                throw new IOException("kaboom!");
            }
        };

        CmdGrep cmdGrep = new CmdGrep();

        Quad quad = toStatement(toIRI("something"), HAS_VERSION, aContentHash);
        cmdGrep.setInputStream(new ByteArrayInputStream(quad.toString().getBytes()));

        cmdGrep.run(blobStoreNull);
    }
}