package bio.guoda.preston.process;

import bio.guoda.preston.store.TestUtil;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.simple.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.RefNodeConstants.QUALIFIED_GENERATION;
import static bio.guoda.preston.RefNodeConstants.USED;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.TripleMatcher.hasTriple;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static junit.framework.TestCase.assertTrue;
import static org.apache.commons.lang3.Range.between;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;

public class BloomFilterDiffTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test(expected = RuntimeException.class)
    public void oneContentMissingBloomFilter() {
        ArrayList<Quad> nodes = new ArrayList<>();
        StatementProcessor processor = new BloomFilterDiff(
                TestUtil.getTestBlobStore(),
                TestUtil.testListener(nodes)
        );

        processor.on(Stream.of(
                toStatement(toIRI("bloom:gz:hash://123abc"), WAS_DERIVED_FROM, toIRI("hash://sha256/aaa"))
        ).collect(Collectors.toList()));
    }

    @Test
    public void oneContentBloomFilter() {
        BlobStoreReadOnly blobStoreReadOnly = key -> {
            if (key.getIRIString().endsWith("hash://123abc")) {
                return new ByteArrayInputStream(writeFilter(generateRandomBloomFilter(1)).toByteArray());
            } else {
                throw new IOException("kaboom!");
            }
        };

        ArrayList<Quad> nodes = new ArrayList<>();
        StatementProcessor processor = new BloomFilterDiff(
                blobStoreReadOnly,
                TestUtil.testListener(nodes)
        );

        processor.on(Stream.of(
                toStatement(
                        toIRI("bloom:gz:hash://123abc"),
                        WAS_DERIVED_FROM,
                        toIRI("hash://sha256/aaa"))
        ).collect(Collectors.toList()));

        assertThat(nodes.size(), is(0));
    }


    @Test
    public void sharedLinks() throws IOException {
        BloomFilter<CharSequence> filter1 = generateRandomBloomFilter(10);
        BloomFilter<CharSequence> filter2 = generateRandomBloomFilter(5);

        filter1.putAll(filter2);

        ByteArrayOutputStream out1 = writeFilter(filter1);
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
        StatementProcessor processor = new BloomFilterDiff(
                blobStoreReadOnly,
                TestUtil.testListener(nodes));

        IRI content1 = toIRI("hash://sha256/aaa");
        IRI bloomHash1 = toIRI("bloom:gz:hash://sha256/123");

        IRI content2 = toIRI("hash://sha256/bbb");
        IRI bloomHash2 = toIRI("bloom:gz:hash://sha256/456");

        processor.on(Stream.of(
                toStatement(bloomHash1, WAS_DERIVED_FROM, content1),
                toStatement(bloomHash2, WAS_DERIVED_FROM, content2)
        ).collect(Collectors.toList()));

        assertThat(nodes.size(), is(7));

        long expectedScore = 5L;
        assertThat(nodes.get(1).getPredicate(), is(HAS_VALUE));
        assertThat(nodes.get(1).getObject(), is(toLiteral(Long.toString(expectedScore), Types.XSD_LONG)));

        assertThat(nodes.get(2).getPredicate(), is(QUALIFIED_GENERATION));
        IRI generationId = (IRI) nodes.get(2).getObject();

        assertThat(nodes.get(3), hasTriple(toStatement(generationId, USED, content2)));
        assertThat(nodes.get(4), hasTriple(toStatement(generationId, USED, bloomHash2)));
        assertThat(nodes.get(5), hasTriple(toStatement(generationId, USED, content1)));
        assertThat(nodes.get(6), hasTriple(toStatement(generationId, USED, bloomHash1)));
    }

    public static ByteArrayOutputStream writeFilter(BloomFilter<CharSequence> filter2) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream out1 = new GZIPOutputStream(out);
        filter2.writeTo(out1);
        out1.flush();
        out1.close();
        return out;
    }


    @Test
    public void createBloomFilter() throws IOException {
        BloomFilter<CharSequence> filter = BloomFilter
                .create(Funnels.stringFunnel(StandardCharsets.UTF_8),
                        10 * 1000000);

        assertFalse(filter.mightContain("bla"));

        filter.put("bla");

        assertTrue(filter.mightContain("bla"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        filter.writeTo(out);
        assertThat(out.size(), is(9123062));

        BloomFilter<CharSequence> restoredFilter = BloomFilter.readFrom(new ByteArrayInputStream(out.toByteArray()), Funnels.stringFunnel(StandardCharsets.UTF_8));

        assertTrue(restoredFilter.mightContain("bla"));
    }

    @Test
    public void createBloomFilterMany() throws IOException {
        BloomFilter<CharSequence> filter = generateRandomBloomFilter(1000);
        BloomFilter<CharSequence> filter2 = generateRandomBloomFilter(1000);

        assertThat(filter.approximateElementCount(), is(1000L));

        filter.putAll(filter2);

        assertThat(filter.approximateElementCount(), is(2000L));

    }

    @Test
    public void createBloomFilterManySaturated() throws IOException {
        BloomFilter<CharSequence> filter = generateRandomBloomFilter(1000 * 10000);
        BloomFilter<CharSequence> filter2 = generateRandomBloomFilter(1000 * 10000);


        filter.putAll(filter2);

        assertThat(filter.approximateElementCount(), allOf(greaterThan(1900L * 10000), lessThan(2100L * 10000)));

    }

    @Test
    public void createBloomFilterManySaturatedCompression() throws IOException {
        BloomFilter<CharSequence> filter0 = generateRandomBloomFilter(0);
        BloomFilter<CharSequence> filter1 = generateRandomBloomFilter(10 * 10000);
        BloomFilter<CharSequence> filter2 = generateRandomBloomFilter(10 * 10000);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (OutputStream out = new GZIPOutputStream(bytes)) {
            filter0.writeTo(out);
        }

        assertThat(bytes.size(), is(lessThan(9000)));
        assertThat(filter0.approximateElementCount(), is(0L));

        filter0.putAll(filter1);

        bytes = new ByteArrayOutputStream();
        try (OutputStream out = new GZIPOutputStream(bytes)) {
            filter0.writeTo(out);
        }

        assertThat(bytes.size(), lessThan(1024 * 1024));

        filter0.putAll(filter2);

        bytes = new ByteArrayOutputStream();
        try (OutputStream out = new GZIPOutputStream(bytes)) {
            filter0.writeTo(out);
        }

        assertThat(bytes.size(), greaterThan(1024 * 1024));

    }

    private BloomFilter<CharSequence> generateRandomBloomFilter(int numberOfElements) {
        BloomFilter<CharSequence> filter = BloomFilter
                .create(Funnels.stringFunnel(StandardCharsets.UTF_8),
                        10 * 1000000);

        for (int i = 0; i < numberOfElements; i++) {
            filter.put(UUID.randomUUID().toString());
        }
        return filter;
    }

}