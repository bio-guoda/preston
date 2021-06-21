package bio.guoda.preston.process;

import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import bio.guoda.preston.store.TestUtilForProcessor;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.simple.Types;
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
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
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.Is.is;

public class SketchIntersectThetaTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test(expected = RuntimeException.class)
    public void oneContentMissingBloomFilter() {
        ArrayList<Quad> nodes = new ArrayList<>();
        StatementProcessor processor = new SketchIntersectTheta(
                TestUtil.getTestBlobStore(),
                TestUtilForProcessor.testListener(nodes)
        );

        processor.on(Stream.of(
                toStatement(toIRI("theta:hash://123abc"), WAS_DERIVED_FROM, toIRI("hash://sha256/aaa"))
        ).collect(Collectors.toList()));
    }

    @Test
    public void oneContentBloomFilter() {
        BlobStoreReadOnly blobStoreReadOnly = key -> {
            if (key.getIRIString().endsWith("hash://123abc")) {
                return new ByteArrayInputStream(writeFilter(generateSketch(1)).toByteArray());
            } else {
                throw new IOException("kaboom!");
            }
        };

        ArrayList<Quad> nodes = new ArrayList<>();
        StatementProcessor processor = new SketchIntersectTheta(
                blobStoreReadOnly,
                TestUtilForProcessor.testListener(nodes)
        );

        processor.on(Stream.of(
                toStatement(
                        toIRI("theta:hash://123abc"),
                        WAS_DERIVED_FROM,
                        toIRI("hash://sha256/aaa"))
        ).collect(Collectors.toList()));

        assertThat(nodes.size(), is(0));
    }


    @Test
    public void sharedLinks() throws IOException {
        Sketch filter1 = generateSketch(10);
        Sketch filter2 = generateSketch(5);

        Union union = SetOperation.builder().buildUnion();
        Sketch unionResult = union.union(filter1, filter2);

        ByteArrayOutputStream out1 = writeFilter(unionResult);
        ByteArrayOutputStream out2 = writeFilter(filter2);


        BlobStoreReadOnly blobStoreReadOnly = key -> {
            if (key.getIRIString().endsWith("123")) {
                return new ByteArrayInputStream(out1.toByteArray());
            } else if (key.getIRIString().endsWith("456")) {
                return new ByteArrayInputStream(out2.toByteArray());
            } else {
                throw new IOException("kaboom!");
            }
        };


        ArrayList<Quad> nodes = new ArrayList<>();
        StatementProcessor processor = new SketchIntersectTheta(
                blobStoreReadOnly,
                TestUtilForProcessor.testListener(nodes));

        IRI content1 = toIRI("hash://sha256/aaa");
        IRI bloomHash1 = toIRI("theta:hash://sha256/123");

        IRI content2 = toIRI("hash://sha256/bbb");
        IRI bloomHash2 = toIRI("theta:hash://sha256/456");

        processor.on(Stream.of(
                toStatement(bloomHash1, WAS_DERIVED_FROM, content1),
                toStatement(bloomHash2, WAS_DERIVED_FROM, content2)
        ).collect(Collectors.toList()));

        assertThat(nodes.size(), is(9));

        assertThat(nodes.get(1).toString(), startsWith("<hash://sha256/bbb> <http://purl.obolibrary.org/obo/RO_0002131> <hash://sha256/aaa>"));

        assertThat(nodes.get(2).getPredicate(), is(HAS_VALUE));
        assertThat(nodes.get(2).getObject(), is(toLiteral("5.00", Types.XSD_DOUBLE)));

        assertThat(nodes.get(3).getPredicate(), is(CONFIDENCE_INTERVAL_95));
        assertThat(nodes.get(3).getObject(), is(toLiteral("5.00", Types.XSD_DOUBLE)));

        assertThat(nodes.get(4).getPredicate(), is(QUALIFIED_GENERATION));
        IRI generationId = (IRI) nodes.get(4).getObject();

        assertThat(nodes.get(5), hasTriple(toStatement(generationId, USED, content2)));
        assertThat(nodes.get(6), hasTriple(toStatement(generationId, USED, bloomHash2)));
        assertThat(nodes.get(7), hasTriple(toStatement(generationId, USED, content1)));
        assertThat(nodes.get(8), hasTriple(toStatement(generationId, USED, bloomHash1)));
    }

    public static ByteArrayOutputStream writeFilter(Sketch filter2) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.write(filter2.compact().toByteArray(), out);
        return out;
    }


    @Test
    public void createSketch() throws IOException {
        UpdateSketch filter0 = UpdateSketch.builder().build();

        UpdateSketch filter1 = UpdateSketch.builder().build();
        filter1.update("bla");

        Intersection intersection = SetOperation.builder().buildIntersection();
        CompactSketch intersect = intersection.intersect(filter0, filter1);

        assertThat(intersect.getEstimate(), is(0.0d));

        filter0.update("bla");

        CompactSketch intersect1 = intersection.intersect(filter0, filter1);
        assertThat(intersect1.getEstimate(), is(1.0d));


        byte[] bytes = intersect1.toByteArray();
        assertThat(bytes.length, is(16));

        Sketch restoredSketch = Sketches.wrapSketch(Memory.wrap(bytes));

        assertThat(intersection.intersect(restoredSketch, filter1).getEstimate(), is(1.0d));
    }

    @Test
    public void createBloomFilterMany() throws IOException {
        Sketch filter = generateSketch(1000);
        Sketch filter2 = generateSketch(1000);

        assertThat(filter.getEstimate(), is(1000.0d));

        Union union = SetOperation.builder().buildUnion();
        Sketch unionResult = union.union(filter, filter2);

        assertThat(unionResult.getEstimate(), is(2000.0d));

    }

    @Test
    public void createSketchesManySaturated() throws IOException {


        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        for (int i = 0; i < 2 * 10 * 1000 * 1000; i++) {
            UUID.randomUUID().toString();
        }
        stopWatch.stop();
        long uuidOverheadEstimate = stopWatch.getTime();

        stopWatch.reset();
        stopWatch.start();

        Sketch filter = generateSketch(10 * 1000 * 1000);
        Sketch filter2 = generateSketch(10 * 1000 * 1000);

        stopWatch.stop();

        System.out.println("generating 2 theta sketches with 20*10^6 elements each took [" + (stopWatch.getTime() - uuidOverheadEstimate) / 1000.0 + "]s");


        stopWatch.reset();
        stopWatch.start();
        Union union = SetOperation.builder().buildUnion();
        Sketch unionResult = union.union(filter, filter2);

        stopWatch.stop();
        System.out.println("calculating union of 2 theta sketches with 20*10^6 elements each took [" + stopWatch.getTime() / 1000.0 + "]s");
        assertThat(unionResult.getEstimate(), allOf(greaterThan(1900 * 10000.0d), lessThan(2100 * 10000d)));

        assertThat((unionResult.getUpperBound(2) - unionResult.getLowerBound(2)) / unionResult.getEstimate(),
                lessThan(0.07d));

    }


    @Test
    public void createBloomFilterManySaturatedCompression() throws IOException {
        UpdateSketch filter0 = generateSketch(0);
        Sketch filter1 = generateSketch(10 * 10000);
        Sketch filter2 = generateSketch(10 * 10000);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        IOUtils.write(filter0.compact().toByteArray(), bytes);

        assertThat(bytes.size(), is(lessThan(9000)));
        assertThat(filter0.getEstimate(), is(0.0d));

        Union union = SetOperation.builder().buildUnion();
        union.union(filter0);
        union.union(filter1);

        bytes = new ByteArrayOutputStream();
        IOUtils.write(union.getResult().compact().toByteArray(), bytes);

        assertThat(bytes.size(), lessThan(1024 * 1024));

        union.union(filter2);

        bytes = new ByteArrayOutputStream();
        IOUtils.write(union.getResult().compact().toByteArray(), bytes);

        assertThat(bytes.size(), greaterThan(31373));

    }

    @Test
    public void createIntersectionSmallAndBigSet() throws IOException {
        UpdateSketch sketchA = UpdateSketch
                .builder()
                .build();

        for (int i = 0; i < 192717; i++) {
            sketchA.update(UUID.randomUUID().toString());
        }

        assertEquals(185429, sketchA.getLowerBound(2), 10000);
        assertEquals(192885, sketchA.getEstimate(), 10000);
        assertEquals(0.02, sketchA.getTheta(), 0.01d);

        UpdateSketch sketchB = UpdateSketch
                .builder()
                .setResizeFactor(ResizeFactor.X1)
                .build();

        for (int i = 0; i < 3; i++) {
            sketchB.update(UUID.randomUUID().toString());
        }

        assertEquals(3, sketchB.getLowerBound(2), 0);
        assertEquals(3, sketchB.getEstimate(), 0);
        assertEquals(1, sketchB.getTheta(), 0.01d);
    }

    @Test
    public void compareRealLifeSketchesFalseNegative() throws IOException {
        //see https://github.com/bio-guoda/preston/issues/113#issuecomment-816790438

        Sketch sketchA = Sketches.wrapSketch(Memory.wrap(IOUtils.toByteArray(getClass().getResourceAsStream("/bio/guoda/preston/process/sketch/30869702aa9fd66e555b94dd9eefaa653de1bbbc040feff6f40b9b6470769993.theta.bin"))));
        Sketch sketchB = Sketches.wrapSketch(Memory.wrap(IOUtils.toByteArray(getClass().getResourceAsStream("/bio/guoda/preston/process/sketch/544e12d30f8bd789c4b675e31cee8dcf9120b2d5d244fe7da16cac33ee1ec552.theta.bin"))));

        assertEquals(192717d, sketchA.getEstimate(), 1d);
        assertEquals(187063d, sketchA.getLowerBound(2), 1d);
        assertEquals(0.02, sketchA.getTheta(), 0.01d);

        assertEquals(3d, sketchB.getEstimate(), 0.01d);
        assertEquals(3d, sketchB.getLowerBound(2), 0.01d);
        assertEquals(1.0, sketchB.getTheta(), 0.01d);

        CompactSketch intersect = SetOperation
                .builder()
                .buildIntersection()
                .intersect(sketchA, sketchB);

        assertEquals(0.02, intersect.getTheta(), 0.01d);
        assertEquals(0.0, intersect.getEstimate(), 0.01d);
        assertEquals(0.0, intersect.getLowerBound(2), 0.01d);
        assertEquals(163.0, intersect.getUpperBound(2), 0.01d);

        assertThat(intersect.getEstimate(),
                is(0.0d));

    }

    public static UpdateSketch generateSketch(int numberOfElements) {
        UpdateSketch sketch1 = UpdateSketch.builder().build();

        for (int i = 0; i < numberOfElements; i++) {
            sketch1.update(UUID.randomUUID().toString());
        }
        return sketch1;
    }

}