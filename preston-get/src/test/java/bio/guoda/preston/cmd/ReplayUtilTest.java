package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementLoggerNQuads;
import bio.guoda.preston.process.ProcessorStateAlwaysContinue;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.HexaStore;
import bio.guoda.preston.store.TracerOfDescendants;
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

import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;
import static org.hamcrest.MatcherAssert.assertThat;

public class ReplayUtilTest {

    private static final IRI TEST_KEY_IRI = RefNodeFactory.toIRI("test:key");
    private static final IRI TEST_KEY_NEWER_IRI = RefNodeFactory.toIRI("test:key-new");

    @Test
    public void replay() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StatementLoggerNQuads logger = new StatementLoggerNQuads(
                new PrintStream(out, true)
        );
        ReplayUtil.attemptReplay(getBlobStore(), BIODIVERSITY_DATASET_GRAPH, new TracerOfDescendants(getStatementStore(), new ProcessorStateAlwaysContinue()), new VersionRetriever(getBlobStore()), logger);

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8), Is.is(
                "<urn:example:some> <urn:example:other> <urn:example:thing> .\n" +
                        "<urn:example:some> <urn:example:newer> <urn:example:thing> .\n"));
    }

    @Ignore(value = "re-enable after implementing prov root selection")
    @Test
    public void replayNonDefaultProvenanceRoot() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StatementLoggerNQuads logger = new StatementLoggerNQuads(new PrintStream(out, true));
        ReplayUtil.attemptReplay(
                getBlobStore(),
                TEST_KEY_IRI,
                new TracerOfDescendants(getStatementStore(), new ProcessorStateAlwaysContinue()),
                new VersionRetriever(getBlobStore()),
                logger);

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8), Is.is(
                "<urn:example:some> <urn:example:other> <urn:example:thing> .\n" +
                        "<urn:example:some> <urn:example:newer> <urn:example:thing> .\n"));

    }

    @Ignore(value = "re-enable after implementing prov root selection")
    @Test
    public void replayNonDefaultProvenanceRootHead() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StatementLoggerNQuads logger = new StatementLoggerNQuads(new PrintStream(out, true));
        ReplayUtil.attemptReplay(
                getBlobStore(),
                TEST_KEY_NEWER_IRI,
                new TracerOfDescendants(getStatementStore(), new ProcessorStateAlwaysContinue()),
                new VersionRetriever(getBlobStore()),
                logger
        );

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8), Is.is(
                "<urn:example:some> <urn:example:newer> <urn:example:thing> .\n"));

    }

    @Test
    public void replayNonExistingProvenanceRoot() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StatementLoggerNQuads logger = new StatementLoggerNQuads(new PrintStream(out, true));
        ReplayUtil.attemptReplay(
                getBlobStore(),
                RefNodeFactory.toIRI("non-existing"),
                new TracerOfDescendants(getStatementStore(), new ProcessorStateAlwaysContinue()),
                new VersionRetriever(getBlobStore()),
                logger
        );

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8), Is.is(""));

    }


    public HexaStore getStatementStore() {
        return new HexaStore() {
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
                    return IOUtils.toInputStream("<urn:example:some> <urn:example:other> <urn:example:thing> .", StandardCharsets.UTF_8);
                } else if (key.equals(TEST_KEY_NEWER_IRI)) {
                    return IOUtils.toInputStream("<urn:example:some> <urn:example:newer> <urn:example:thing> .", StandardCharsets.UTF_8);
                } else {
                    throw new IOException("no value for [" + key.getIRIString() + "] found.");
                }
            }
        };
    }


}