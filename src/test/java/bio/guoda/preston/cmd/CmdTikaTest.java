package bio.guoda.preston.cmd;


import bio.guoda.preston.HashGeneratorTLSHTruncatedTest;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.store.BlobStore;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class CmdTikaTest {

    @Test
    public void processOneVersion() throws IOException {
        IRI aContentHash = toIRI("hash://sha256/blabla");

        BlobStoreNull blobStoreNull = new BlobStoreNull(){
            @Override
            public InputStream get(IRI key) throws IOException {
                if (getAttemptCount.incrementAndGet() > 2 || !aContentHash.equals(key)) {
                    throw new IOException("kaboom!");
                }
                return getClass().getResourceAsStream(HashGeneratorTLSHTruncatedTest.DWCA);
            }
        };

        StatementStoreNull statementStoreNull = new StatementStoreNull();

        CmdTika cmdTika = new CmdTika();

        Quad quad = toStatement(
                toIRI("something"),
                toIRI("isRelatedTo"),
                aContentHash
        );
        cmdTika.setInputStream(new ByteArrayInputStream(quad.toString().getBytes()));

        cmdTika.run(blobStoreNull, statementStoreNull);

        assertThat(blobStoreNull.getAttemptCount.get(), is(1));
    }
}