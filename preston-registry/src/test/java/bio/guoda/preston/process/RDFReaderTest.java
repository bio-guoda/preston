package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.TestUtilForProcessor;
import org.apache.commons.io.input.NullInputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class RDFReaderTest {

    @Test
    public void recordProvVersions() {
        Stream<Quad> quads = Stream.of(
                toStatement(toIRI("bloop"), RefNodeConstants.HAS_VERSION, toIRI("hash://sha256/bloop")),
                toStatement(toIRI("hash://sha256/blip"), RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("hash://sha256/bloop")),
                toStatement(toIRI("blup"), RefNodeConstants.USED, toIRI("hash://sha256/blip"))
        );

        LinkedList<Quad> nodes = new LinkedList<>();

        IRI qualifiedGeneration = toIRI("00000000-0000-0000-0000-000000000000");
        RDFReader indexer = new RDFReader(
                new TestBlobStore(),
                TestUtilForProcessor.testListener(nodes),
                qualifiedGeneration,
                (origin) -> new StatementsEmitterAdapter() {
                    @Override
                    public void emit(Quad statement) {
                        // ignore
                    }
                },
                new ProcessorStateAlwaysContinue()
        );

        quads.forEach(indexer::on);

        assertThat(nodes.size(), is(2));
        assertThat(nodes.get(0).toString(), equalTo("<00000000-0000-0000-0000-000000000000> <http://www.w3.org/ns/prov#used> <hash://sha256/bloop> <00000000-0000-0000-0000-000000000000> ."));
        assertThat(nodes.get(1).toString(), equalTo("<00000000-0000-0000-0000-000000000000> <http://www.w3.org/ns/prov#used> <hash://sha256/blip> <00000000-0000-0000-0000-000000000000> ."));
    }

    private static class TestBlobStore implements BlobStore {
        @Override
        public IRI put(InputStream is) throws IOException {
            return Hasher.calcHashIRI(is, NullOutputStream.NULL_OUTPUT_STREAM, HashType.sha256);
        }

        @Override
        public InputStream get(IRI key) {
            return new NullInputStream();
        }
    }
}