package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.EmittingStreamFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.ParsingEmitter;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementLoggerNQuads;
import bio.guoda.preston.process.ProcessorStateAlwaysContinue;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.HexaStore;
import bio.guoda.preston.store.ProvenanceTracer;
import bio.guoda.preston.store.ProvenanceTracerByIndex;
import bio.guoda.preston.store.ProvenanceTracerImpl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;
import static bio.guoda.preston.RefNodeConstants.USED_BY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;

public class ReplayUtilTest {

    private static final IRI TEST_KEY_IRI = RefNodeFactory.toIRI("hash://sha256/5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03");
    private static final IRI TEST_KEY_NEWER_IRI = RefNodeFactory.toIRI("hash://sha256/aaa1b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03");

    @Test
    public void replay() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StatementLoggerNQuads logger = new StatementLoggerNQuads(
                new PrintStream(out, true)
        );
        ReplayUtil.attemptReplay(
                getBlobStore(),
                BIODIVERSITY_DATASET_GRAPH,
                new ProvenanceTracerByIndex(getStatementStore(), new ProvenanceTracerImpl(getBlobStore())), new EmittingStreamFactory() {
                    @Override
                    public ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context) {
                        return new EmittingStreamOfAnyVersions(emitter, context);
                    }
                },
                new VersionRetriever(getBlobStore()), logger);

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8), endsWith(
                "<http://purl.org/pav/hasVersion> <hash://sha256/5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03> .\n"));
    }

    @Test
    public void replayNonDefaultProvenanceRoot() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StatementLoggerNQuads logger = new StatementLoggerNQuads(new PrintStream(out, true));
        ReplayUtil.attemptReplay(
                getBlobStore(),
                TEST_KEY_IRI,
                new ProvenanceTracerByIndex(getStatementStore(), new ProvenanceTracerImpl(getBlobStore())), new EmittingStreamFactory() {
                    @Override
                    public ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context) {
                        return new EmittingStreamOfAnyVersions(emitter, context);
                    }
                },
                new VersionRetriever(getBlobStore()),
                logger);

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8), endsWith(
                "<http://purl.org/pav/hasVersion> <hash://sha256/5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03> .\n"));
    }

    @Test
    public void replayNonDefaultProvenanceRootHead() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StatementLoggerNQuads logger = new StatementLoggerNQuads(new PrintStream(out, true));
        ReplayUtil.attemptReplay(
                getBlobStore(),
                TEST_KEY_NEWER_IRI,
                new ProvenanceTracerByIndex(getStatementStore(), new ProvenanceTracerImpl(getBlobStore())), new EmittingStreamFactory() {
                    @Override
                    public ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context) {
                        return new EmittingStreamOfAnyVersions(emitter, context);
                    }
                },
                new VersionRetriever(getBlobStore()),
                logger
        );

        assertThat(new String(out.toByteArray(), StandardCharsets.UTF_8),
                endsWith("<http://purl.org/pav/hasVersion> <hash://sha256/5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03> .\n"));

    }

    @Test
    public void replayNonExistingProvenanceRoot() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StatementLoggerNQuads logger = new StatementLoggerNQuads(new PrintStream(out, true));
        ReplayUtil.attemptReplay(
                getBlobStore(),
                RefNodeFactory.toIRI("non-existing"),
                new ProvenanceTracerByIndex(getStatementStore(), new ProvenanceTracer() {
                    @Override
                    public void trace(IRI provenanceAnchor, StatementListener listener) throws IOException {

                    }
                }), new EmittingStreamFactory() {
                    @Override
                    public ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context) {
                        return new EmittingStreamOfAnyVersions(emitter, context);
                    }
                },
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
                    return IOUtils.toInputStream(
                            RefNodeFactory.toStatement(TEST_KEY_IRI, USED_BY, RefNodeFactory.toIRI("foo:bar"))
                                    + "\n<urn:example:some> <urn:example:newer> <urn:example:thing> .", StandardCharsets.UTF_8);
                } else {
                    throw new IOException("no value for [" + key.getIRIString() + "] found.");
                }
            }
        };
    }


}