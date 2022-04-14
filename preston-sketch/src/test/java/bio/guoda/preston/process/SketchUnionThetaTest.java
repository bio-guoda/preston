package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.StatementProcessor;
import bio.guoda.preston.store.TestUtil;
import bio.guoda.preston.store.TestUtilForProcessor;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.simple.Types;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.CONFIDENCE_INTERVAL_95;
import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.RefNodeConstants.QUALIFIED_GENERATION;
import static bio.guoda.preston.RefNodeConstants.USED;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.TripleMatcher.hasTriple;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toLiteral;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static bio.guoda.preston.process.SketchIntersectThetaTest.generateSketch;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class SketchUnionThetaTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test(expected = RuntimeException.class)
    public void missingThetaSketchHash() {
        ArrayList<Quad> nodes = new ArrayList<>();
        StatementProcessor processor = new SketchUnionTheta(
                TestUtil.getTestBlobStore(HashType.sha256),
                TestUtilForProcessor.testListener(nodes)
        );

        processor.on(Stream.of(
                toStatement(toIRI("theta:hash://123abc"), WAS_DERIVED_FROM, toIRI("hash://sha256/aaa"))
        ).collect(Collectors.toList()));
    }

    @Test
    public void singleSketch() throws IOException {
        BlobStoreReadOnly blobStoreReadOnly = key -> {
            if (key.getIRIString().endsWith("hash://123abc")) {
                return new ByteArrayInputStream(writeFilter(generateSketch(1)).toByteArray());
            } else {
                throw new IOException("kaboom!");
            }
        };

        IRI sketchA = toIRI("theta:hash://123abc");
        ArrayList<Quad> nodes = new ArrayList<>();
        try (SketchUnionTheta processor = new SketchUnionTheta(
                blobStoreReadOnly,
                TestUtilForProcessor.testListener(nodes)
        )) {

            processor.on(Stream.of(
                    toStatement(
                            sketchA,
                            WAS_DERIVED_FROM,
                            toIRI("hash://sha256/aaa"))
            ).collect(Collectors.toList()));
        }
        assertThat(nodes.size(), is(6));

        assertThat(nodes.get(3).getPredicate(), is(HAS_VALUE));
        assertThat(nodes.get(3).getObject(), is(toLiteral("1.00", Types.XSD_DOUBLE)));

        assertThat(nodes.get(4).getPredicate(), is(CONFIDENCE_INTERVAL_95));
        assertThat(nodes.get(4).getObject(), is(toLiteral("1.00", Types.XSD_DOUBLE)));

        assertThat(nodes.get(5).getPredicate(), is(QUALIFIED_GENERATION));
        IRI generationId = (IRI) nodes.get(5).getObject();

        assertThat(nodes.get(2), hasTriple(toStatement(generationId, USED, sketchA)));

    }


    @Test
    public void twoSketches() throws IOException {
        Sketch sketchA = generateSketch(10);
        Sketch sketchB = generateSketch(5);

        Union union = SetOperation.builder().buildUnion();
        Sketch unionResult = union.union(sketchA, sketchB);

        ByteArrayOutputStream out1 = writeFilter(unionResult);
        ByteArrayOutputStream out2 = writeFilter(sketchB);


        BlobStoreReadOnly blobStoreReadOnly = key -> {
            if (key.getIRIString().endsWith("123")) {
                return new ByteArrayInputStream(out1.toByteArray());
            } else if (key.getIRIString().endsWith("456")) {
                return new ByteArrayInputStream(out2.toByteArray());
            } else {
                throw new IOException("kaboom!");
            }
        };

        IRI content1 = toIRI("hash://sha256/aaa");
        IRI sketchHashB = toIRI("theta:hash://sha256/123");

        IRI content2 = toIRI("hash://sha256/bbb");
        IRI sketchHashA = toIRI("theta:hash://sha256/456");

        ArrayList<Quad> nodes = new ArrayList<>();
        try (SketchUnionTheta processor = new SketchUnionTheta(
                blobStoreReadOnly,
                TestUtilForProcessor.testListener(nodes))) {


            processor.on(Stream.of(
                    toStatement(sketchHashB, WAS_DERIVED_FROM, content1),
                    toStatement(sketchHashA, WAS_DERIVED_FROM, content2)
            ).collect(Collectors.toList()));
        }
        assertThat(nodes.size(), is(7));

        assertThat(nodes.get(4).getPredicate(), is(HAS_VALUE));
        assertThat(nodes.get(5).getObject(), is(toLiteral("15.00", Types.XSD_DOUBLE)));

        assertThat(nodes.get(5).getPredicate(), is(CONFIDENCE_INTERVAL_95));
        assertThat(nodes.get(5).getObject(), is(toLiteral("15.00", Types.XSD_DOUBLE)));

        assertThat(nodes.get(6).getPredicate(), is(QUALIFIED_GENERATION));
        IRI generationId = (IRI) nodes.get(6).getObject();

        assertThat(nodes.get(3), hasTriple(toStatement(generationId, USED, sketchHashA)));
        assertThat(nodes.get(2), hasTriple(toStatement(generationId, USED, sketchHashB)));
    }

    public static ByteArrayOutputStream writeFilter(Sketch filter2) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.write(filter2.compact().toByteArray(), out);
        return out;
    }


}