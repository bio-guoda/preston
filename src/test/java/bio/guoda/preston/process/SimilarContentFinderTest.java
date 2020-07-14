package bio.guoda.preston.process;

import bio.guoda.preston.store.TestUtil;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.simple.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.RefNodeConstants.QUALIFIED_GENERATION;
import static bio.guoda.preston.RefNodeConstants.USED;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.TripleMatcher.hasTriple;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class SimilarContentFinderTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void oneContent() throws IOException {
        ArrayList<Quad> nodes = new ArrayList<>();
        StatementProcessor processor = new SimilarContentFinder(TestUtil.getTestBlobStore(), TestUtil.testListener(nodes), tmpDir.newFolder(), 10, 0f);

        processor.on(Stream.of(
                toStatement(toIRI("hash://tika-tlsh/fff"), WAS_DERIVED_FROM, toIRI("hash://sha256/aaa"))
        ).collect(Collectors.toList()));

        assertThat(nodes.size(), is(0));
    }

    @Test
    public void similarContent() throws IOException {
        ArrayList<Quad> nodes = new ArrayList<>();
        StatementProcessor processor = new SimilarContentFinder(TestUtil.getTestBlobStore(), TestUtil.testListener(nodes), tmpDir.newFolder(), 10, 0f);

        IRI content1 = toIRI("hash://sha256/aaa");
        IRI tlsh1 = toIRI("hash://tika-tlsh/beef");

        IRI content2 = toIRI("hash://sha256/bbb");
        IRI tlsh2 = toIRI("hash://tika-tlsh/beaf");

        processor.on(Stream.of(
                toStatement(tlsh1, WAS_DERIVED_FROM, content1),
                toStatement(tlsh2, WAS_DERIVED_FROM, content2)
        ).collect(Collectors.toList()));

        assertThat(nodes.size(), is(5));

        float expectedScore = GetSimilarityScore(tlsh2, tlsh2);
        assertThat(nodes.get(1).getPredicate(), is(HAS_VALUE));
        assertThat(nodes.get(1).getObject(), is(toLiteral(Float.toString(expectedScore), Types.XSD_FLOAT)));

        assertThat(nodes.get(2).getPredicate(), is(QUALIFIED_GENERATION));
        IRI generationId = (IRI)nodes.get(2).getObject();

        assertThat(nodes.get(3), hasTriple(toStatement(generationId, USED, content2)));
        assertThat(nodes.get(4), hasTriple(toStatement(generationId, USED, content1)));
    }

    @Test
    public void differentContent() throws IOException {
        ArrayList<Quad> nodes = new ArrayList<>();
        StatementProcessor processor = new SimilarContentFinder(TestUtil.getTestBlobStore(), TestUtil.testListener(nodes), tmpDir.newFolder(), 10, 0f);

        processor.on(Stream.of(
                toStatement(toIRI("hash://tika-tlsh/fff"), WAS_DERIVED_FROM, toIRI("hash://sha256/aaa")),
                toStatement(toIRI("hash://tika-tlsh/222"), WAS_DERIVED_FROM, toIRI("hash://sha256/bbb"))
        ).collect(Collectors.toList()));

        assertThat(nodes.size(), is(0));
    }

    private float GetSimilarityScore(IRI tlsh1, IRI tlsh2) {
        return 7f;
    }
}