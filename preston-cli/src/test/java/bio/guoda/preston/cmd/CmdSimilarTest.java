package bio.guoda.preston.cmd;

import bio.guoda.preston.RDFUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class CmdSimilarTest {

    @Test
    public void cleanTmpFiles() {
        BlobStoreNull blobStoreNull = new BlobStoreNull();
        StatementStoreNull statementStoreNull = new StatementStoreNull();

        CmdSimilar cmdSimilar = new CmdSimilar();

        cmdSimilar.setInputStream(new ByteArrayInputStream(new byte[0]));
        cmdSimilar.run(blobStoreNull, statementStoreNull);

        assertThat((new File(cmdSimilar.getIndexPath())).exists(), is(false));
    }

    @Test
    public void twoSimilarContents() throws IOException {
        IRI contentHash1 = toIRI("hash://sha256/blabla");
        IRI similarityHash1 = toIRI("hash://tika-tlsh/22c9a6d80630687c345faa7e08a4bff27d26b6370ce21781c493d474bf765a7548c");

        IRI contentHash2 = toIRI("hash://sha256/blublub");
        IRI similarityHash2 = toIRI("hash://tika-tlsh/22c9a6d80630687c345faa7e08a4bff27d26b6370ce21781c493d474bf765a7548d");

        BlobStoreNull blobStoreNull = new BlobStoreNull();
        StatementStoreNull statementStoreNull = new StatementStoreNull();

        CmdSimilar cmdSimilar = new CmdSimilar();

        Stream<Quad> quads = Stream.of(
                toStatement(similarityHash1, WAS_DERIVED_FROM, contentHash1),
                toStatement(similarityHash2, WAS_DERIVED_FROM, contentHash2)
        );
        cmdSimilar.setInputStream(new ByteArrayInputStream(quads.map(Quad::toString).collect(Collectors.joining()).getBytes()));

        cmdSimilar.run(blobStoreNull, statementStoreNull);

        String provenanceLog = IOUtils.toString(
                blobStoreNull.mostRecentBlob.toByteArray(),
                StandardCharsets.UTF_8.toString());

        Stream<Quad> valueStatements = RDFUtil.asQuadStream(IOUtils.toInputStream(provenanceLog, StandardCharsets.UTF_8))
                .filter(quad -> quad.getPredicate().equals(HAS_VALUE));
        assertThat(valueStatements.count(), is(1L));
    }
}