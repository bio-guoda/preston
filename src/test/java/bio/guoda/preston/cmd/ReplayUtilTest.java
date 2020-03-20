package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.StatementLoggerNQuads;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.StatementStore;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.hamcrest.core.Is;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertThat;

public class ReplayUtilTest {

    private static final IRI TEST_KEY_IRI = RefNodeFactory.toIRI("test:key");
    private static final IRI TEST_KEY_NEWER_IRI = RefNodeFactory.toIRI("test:key-new");

    @Test
    public void replay() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StatementLoggerNQuads logger = new StatementLoggerNQuads(new PrintStream(out, true));
        ReplayUtil.attemptReplay(getBlobStore(), getStatementStore(), new VersionRetriever(getBlobStore()), logger);

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8), Is.is(
                "<some> <other> <thing> .\n" +
                        "<some> <newer> <thing> .\n"));
    }

    @Ignore(value = "re-enable after implementing prov root selection")
    @Test
    public void replayNonDefaultProvenanceRoot() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StatementLoggerNQuads logger = new StatementLoggerNQuads(new PrintStream(out, true));
        ReplayUtil.attemptReplay(getBlobStore(), getStatementStore(), TEST_KEY_IRI, new VersionRetriever(getBlobStore()), logger);

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8), Is.is(
                "<some> <other> <thing> .\n" +
                        "<some> <newer> <thing> .\n"));

    }

    @Ignore(value = "re-enable after implementing prov root selection")
    @Test
    public void replayNonDefaultProvenanceRootHead() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StatementLoggerNQuads logger = new StatementLoggerNQuads(new PrintStream(out, true));
        ReplayUtil.attemptReplay(getBlobStore(), getStatementStore(), TEST_KEY_NEWER_IRI, new VersionRetriever(getBlobStore()), logger);

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8), Is.is(
                "<some> <newer> <thing> .\n"));

    }

    @Test
    public void replayNonExistingProvenanceRoot() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StatementLoggerNQuads logger = new StatementLoggerNQuads(new PrintStream(out, true));
        ReplayUtil.attemptReplay(getBlobStore(), getStatementStore(), RefNodeFactory.toIRI("non-existing"), new VersionRetriever(getBlobStore()), logger);

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8), Is.is(""));

    }


    public StatementStore getStatementStore() {
        return new StatementStore() {
            @Override
            public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {
                throw new IllegalArgumentException();
            }

            @Override
            public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
                if (queryKey.getRight().equals(RefNodeConstants.HAS_VERSION)
                        && queryKey.getLeft().equals(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH)) {
                    return TEST_KEY_IRI;
                } else if (queryKey.getLeft().equals(RefNodeConstants.HAS_PREVIOUS_VERSION)
                        && queryKey.getRight().equals(TEST_KEY_IRI)) {
                    return TEST_KEY_NEWER_IRI;
                } else {
                    return null;
                }
            }
        };
    }

    public BlobStore getBlobStore() {
        return new BlobStore() {
            @Override
            public IRI put(InputStream is) throws IOException {
                throw new IllegalArgumentException();
            }

            @Override
            public InputStream get(IRI key) throws IOException {
                if (key.equals(TEST_KEY_IRI)) {
                    return IOUtils.toInputStream("<some> <other> <thing> .", StandardCharsets.UTF_8);
                } else if (key.equals(TEST_KEY_NEWER_IRI)) {
                    return IOUtils.toInputStream("<some> <newer> <thing> .", StandardCharsets.UTF_8);
                } else {
                    throw new IOException("no value for [" + key.getIRIString() + "] found.");
                }
            }
        };
    }


}