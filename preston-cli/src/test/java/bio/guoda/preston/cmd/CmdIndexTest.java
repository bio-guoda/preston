package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.TestUtilForProcessor;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;

public class CmdIndexTest {

    public static final String PROV = "/bio/guoda/preston/process/archivetest.nq";

    @Test
    public void testRun() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        CmdIndex cmd = new CmdIndex();

        Quad quad = toStatement(toIRI("something"), HAS_VERSION, toIRI("hash://sha256/blabla"));
        cmd.setInputStream(new ByteArrayInputStream(quad.toString().getBytes()));

        HexaStoreNull statementStoreNull = new HexaStoreNull();
        BlobStore blobStore = createHashingBlobStoreForResource(PROV, HashType.sha256);

        cmd.run(blobStore, statementStoreNull);

        List<Quad> nodes = new ArrayList<>();
        new EmittingStreamRDF(TestUtilForProcessor.testEmitter(nodes))
                .parseAndEmit(new ByteArrayInputStream(out.toByteArray()));

        assertThat(nodes.size(), is(20));
        String generation = nodes.get(12).getSubject().ntriplesString();

        List<String> strings = nodes.stream().map(Quad::toString).collect(Collectors.toList());
        assertThat(strings.get(12), startsWith(generation + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Generation>"));
        assertThat(strings.get(13), startsWith("<urn:something> <http://purl.org/pav/hasVersion> <hash://sha256/blabla>"));
        assertThat(strings.get(14), startsWith(generation + " <http://www.w3.org/ns/prov#used> <hash://sha256/blabla>"));
        assertThat(strings.get(15), startsWith(generation + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Activity>"));
        assertThat(strings.get(16), startsWith(generation + " <http://www.w3.org/ns/prov#wasInformedBy>"));

        // Don't check the hash of the packaged index; Lucene index construction is not deterministic
        assertThat(strings.get(17), containsString(" <http://www.w3.org/ns/prov#wasGeneratedBy> " + generation));
        assertThat(strings.get(18), containsString("<http://www.w3.org/ns/prov#qualifiedGeneration> " + generation));
        assertThat(strings.get(19), startsWith(generation + " <http://www.w3.org/ns/prov#generatedAtTime>"));
    }

    public BlobStore createHashingBlobStoreForResource(String pathToResource, HashType hashType) {
        return new BlobStore() {
            @Override
            public IRI put(InputStream is) throws IOException {
                return Hasher.calcHashIRI(is, NullOutputStream.NULL_OUTPUT_STREAM, hashType);
            }

            @Override
            public InputStream get(IRI key) {
                return getClass().getResourceAsStream(pathToResource);
            }
        };
    }
}