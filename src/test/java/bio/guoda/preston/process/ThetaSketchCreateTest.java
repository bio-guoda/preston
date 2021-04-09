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
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;

public class ThetaSketchCreateTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void singleValue() throws IOException {

        ArrayList<Quad> nodes = new ArrayList<>();
        try (ThetaSketchCreate processor = new ThetaSketchCreate(
                new TestBlobStore(),
                TestUtil.testListener(nodes)
        )) {
            processor.on(Stream.of(
                    toStatement(toIRI("hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c"), HAS_VALUE, toIRI("foo"))
            ).collect(Collectors.toList()));
        }

        assertThat(nodes.size(), is(4));

        assertThat(nodes.get(1).toString(), startsWith("<theta:put:0> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c>"));

    }

    @Test
    public void singleValueCutIRI() throws IOException {

        ArrayList<Quad> nodes = new ArrayList<>();
        try (ThetaSketchCreate processor = new ThetaSketchCreate(
                new TestBlobStore(),
                TestUtil.testListener(nodes)
        )) {
            processor.on(Stream.of(
                    toStatement(toIRI("cut:hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c!/b1-3"), HAS_VALUE, toIRI("foo"))
            ).collect(Collectors.toList()));
        }
        assertThat(nodes.size(), is(4));

        assertThat(nodes.get(1).toString(), startsWith("<theta:gz:put:0> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c>"));

    }

    @Test
    public void twoValues() throws IOException {
        ArrayList<Quad> nodes = new ArrayList<>();
        try (ThetaSketchCreate processor = new ThetaSketchCreate(
                new TestBlobStore(),
                TestUtil.testListener(nodes)
        )) {

            processor.on(Stream.of(
                    toStatement(toIRI("hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c"), HAS_VALUE, toIRI("foo")),
                    toStatement(toIRI("hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c"), HAS_VALUE, toIRI("bar"))
            ).collect(Collectors.toList()));
        }

        assertThat(nodes.get(1).toString(), startsWith("<theta:put:0> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c>"));

        assertThat(nodes.size(), is(4));

    }

    @Test
    public void twoCutValues() throws IOException {
        ArrayList<Quad> nodes = new ArrayList<>();

        try (ThetaSketchCreate processor = new ThetaSketchCreate(
                new TestBlobStore(),
                TestUtil.testListener(nodes)
        )) {
            processor.on(Stream.of(
                    toStatement(toIRI("hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c"), HAS_VALUE, toIRI("foo")),
                    toStatement(toIRI("cut:hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c!/b12-12"), HAS_VALUE, toIRI("bar"))
            ).collect(Collectors.toList()));
        }

        assertThat(nodes.get(1).toString(), startsWith("<theta:put:0> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c>"));

        assertThat(nodes.size(), is(4));

    }

    @Test
    public void createTwoSketchesTwoValues() throws IOException {
        ArrayList<Quad> nodes = new ArrayList<>();
        try (ThetaSketchCreate processor = new ThetaSketchCreate(
                new TestBlobStore(),
                TestUtil.testListener(nodes)
        )) {

            processor.on(Stream.of(
                    toStatement(toIRI("hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c"), HAS_VALUE, toIRI("foo")),
                    toStatement(toIRI("hash://sha256/fffc2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c"), HAS_VALUE, toIRI("bar"))
            ).collect(Collectors.toList()));
        }
        assertThat(nodes.size(), is(8));

        assertThat(nodes.get(1).toString(), startsWith("<theta:put:0> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/c61c2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c>"));
        assertThat(nodes.get(5).toString(), startsWith("<theta:put:1> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/fffc2622391ae5b8fabe7003c32289342a874d306724f7111e49b2a90d8be56c>"));
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