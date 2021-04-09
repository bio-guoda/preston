package bio.guoda.preston.process;

import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.TestUtil;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;

public class SketchBuilderBloomFilterTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void createBloomFilterSingleValue() throws IOException {

        ArrayList<Quad> nodes = new ArrayList<>();
        try (SketchBuilderBloomFilter processor = new SketchBuilderBloomFilter(
                new TestBlobStore(),
                TestUtil.testListener(nodes)
        )) {
            processor.on(Stream.of(
                    toStatement(toIRI("hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c"), HAS_VALUE, toIRI("foo"))
            ).collect(Collectors.toList()));
        }

        assertThat(nodes.size(), is(4));

        assertThat(nodes.get(1).toString(), startsWith("<bloom:gz:put:0> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c>"));

    }

    @Test
    public void createBloomFilterSingleValueCutIRI() throws IOException {

        ArrayList<Quad> nodes = new ArrayList<>();
        try (SketchBuilderBloomFilter processor = new SketchBuilderBloomFilter(
                new TestBlobStore(),
                TestUtil.testListener(nodes)
        )) {
            processor.on(Stream.of(
                    toStatement(toIRI("cut:hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c!/b1-3"), HAS_VALUE, toIRI("foo"))
            ).collect(Collectors.toList()));
        }
        assertThat(nodes.size(), is(4));

        assertThat(nodes.get(1).toString(), startsWith("<bloom:gz:put:0> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c>"));

    }

    @Test
    public void createBloomFilterTwoValues() throws IOException {
        ArrayList<Quad> nodes = new ArrayList<>();
        try (SketchBuilderBloomFilter processor = new SketchBuilderBloomFilter(
                new TestBlobStore(),
                TestUtil.testListener(nodes)
        )) {

            processor.on(Stream.of(
                    toStatement(toIRI("hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c"), HAS_VALUE, toIRI("foo")),
                    toStatement(toIRI("hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c"), HAS_VALUE, toIRI("bar"))
            ).collect(Collectors.toList()));
        }

        assertThat(nodes.get(1).toString(), startsWith("<bloom:gz:put:0> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c>"));

        assertThat(nodes.size(), is(4));

    }

    @Test
    public void createBloomFilterTwoCutValues() throws IOException {
        ArrayList<Quad> nodes = new ArrayList<>();

        try (SketchBuilderBloomFilter processor = new SketchBuilderBloomFilter(
                new TestBlobStore(),
                TestUtil.testListener(nodes)
        )) {
            processor.on(Stream.of(
                    toStatement(toIRI("hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c"), HAS_VALUE, toIRI("foo")),
                    toStatement(toIRI("cut:hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c!/b12-12"), HAS_VALUE, toIRI("bar"))
            ).collect(Collectors.toList()));
        }

        assertThat(nodes.get(1).toString(), startsWith("<bloom:gz:put:0> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c>"));

        assertThat(nodes.size(), is(4));

    }

    @Test
    public void createTwoBloomFilterTwoValues() throws IOException {
        ArrayList<Quad> nodes = new ArrayList<>();
        try (SketchBuilderBloomFilter processor = new SketchBuilderBloomFilter(
                new TestBlobStore(),
                TestUtil.testListener(nodes)
        )) {

            processor.on(Stream.of(
                    toStatement(toIRI("hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c"), HAS_VALUE, toIRI("foo")),
                    toStatement(toIRI("hash://sha256/fffc2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c"), HAS_VALUE, toIRI("bar"))
            ).collect(Collectors.toList()));
        }
        assertThat(nodes.size(), is(8));

        assertThat(nodes.get(1).toString(), startsWith("<bloom:gz:put:0> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c>"));
        assertThat(nodes.get(5).toString(), startsWith("<bloom:gz:put:1> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/fffc2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c>"));
    }


    private static class TestBlobStore implements BlobStore {
        private AtomicInteger counter = new AtomicInteger(0);

        @Override
        public IRI put(InputStream is) throws IOException {
            return RefNodeFactory.toIRI("put:" + counter.getAndIncrement());
        }

        @Override
        public InputStream get(IRI key) throws IOException {
            return null;
        }
    }
}