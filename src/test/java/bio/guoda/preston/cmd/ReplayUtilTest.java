package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.StatementStore;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ReplayUtilTest {

    @Test
    public void replay() {
        final IRI testKeyIRI = RefNodeFactory.toIRI("test:key");
        final IRI testKeyNewerIRI = RefNodeFactory.toIRI("test:key-new");

        final BlobStore blobStore = new BlobStore() {
            @Override
            public IRI putBlob(InputStream is) throws IOException {
                throw new IllegalArgumentException();
            }

            @Override
            public IRI putBlob(RDFTerm entity) throws IOException {
                throw new IllegalArgumentException();
            }

            @Override
            public InputStream get(IRI key) throws IOException {
                if (key.equals(testKeyIRI)) {
                    return IOUtils.toInputStream("<some> <other> <thing> .", StandardCharsets.UTF_8);
                } else if (key.equals(testKeyNewerIRI)) {
                    return IOUtils.toInputStream("<some> <newer> <thing> .", StandardCharsets.UTF_8);
                } else {
                    throw new IOException("no value for [" + key.getIRIString() + "] found.");
                }
            }
        };
        ReplayUtil.attemptReplay(blobStore, new StatementStore() {
            @Override
            public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {
                throw new IllegalArgumentException();
            }

            @Override
            public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
                System.out.println(queryKey.toString());
                if (queryKey.getRight().equals(RefNodeConstants.HAS_VERSION)) {
                    return testKeyIRI;
                } else if (queryKey.getLeft().equals(RefNodeConstants.HAS_PREVIOUS_VERSION)) {
                    return testKeyNewerIRI;
                } else {
                    return null;
                }
            }
        }, new VersionRetriever(blobStore), StatementLogFactory.createLogger(Logger.nquads));
    }

}