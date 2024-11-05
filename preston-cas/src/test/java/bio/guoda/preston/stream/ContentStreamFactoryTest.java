package bio.guoda.preston.stream;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Ignore;
import org.junit.Test;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.zip.GZIPInputStream;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class ContentStreamFactoryTest {

    @Test(expected = IllegalArgumentException.class)
    public void noContentStreamForNonContentHash() throws IOException {
        new ContentStreamFactory(RefNodeFactory.toIRI("foo:bar"));
    }

    @Test
    public void matchingGZipPrefix() throws IOException {

        assertTrue(ContentStreamFactory.hasSupportedCompressionPrefix(  
                RefNodeFactory.toIRI("gz:something"),
                RefNodeFactory.toIRI("gz:something!/something"))
        );
    }

    @Test
    public void matchingBZip2Prefix() throws IOException {
        assertTrue(ContentStreamFactory.hasSupportedCompressionPrefix(
                RefNodeFactory.toIRI("bzip2:something"),
                RefNodeFactory.toIRI("bzip2:something!/something"))
        );
    }

    @Test
    public void notMatchingGZipPrefix() throws IOException {

        assertFalse(ContentStreamFactory.hasSupportedCompressionPrefix(
                RefNodeFactory.toIRI("gz:else"),
                RefNodeFactory.toIRI("gz:something!/something"))
        );
    }

    @Test
    public void contentStreamForContentHash() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        InputStream inputStream = factory.create(getZipArchiveStream());
        assertNotNull(inputStream);
    }

    public InputStream getZipArchiveStream() {
        return getClass().getResourceAsStream("/bio/guoda/preston/process/nested.zip");
    }

    public InputStream getPDFStream() {
        return getClass().getResourceAsStream("/bio/guoda/preston/process/elliott2023.pdf");
    }

    public InputStream getPDFStreamKoopman() {
        return getClass().getResourceAsStream("/bio/guoda/preston/process/koopman1994.pdf");
    }

    @Test
    public void contentStreamForEmbeddedContent() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("zip:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/level1.txt"));
        InputStream inputStream = factory.create(getZipArchiveStream());
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("https://example.org"));
    }

    @Test
    public void contentStreamForCutEmbeddedContent() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("cut:zip:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/level1.txt!/b1-5"));
        InputStream inputStream = factory.create(getZipArchiveStream());
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("https"));
    }

    @Test
    public void contentStreamForBZIP2EmbeddedContent() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("bzip2:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/something.txt"));
        InputStream resourceAsStream = getClass().getResourceAsStream("foo.txt.bz2");
        assertNotNull(resourceAsStream);
        InputStream inputStream = factory.create(resourceAsStream);
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("bar"));
    }

    @Test
    public void contentStreamForGZIPEmbeddedContent() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("gz:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/something.txt"));
        InputStream inputStream = factory.create(getClass().getResourceAsStream("foo.txt.gz"));
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("bar"));
    }

    @Test
    public void contentStreamForGZIPEmbeddedContentManyLines() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("gz:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/something.txt"));
        InputStream inputStream = factory.create(getClass().getResourceAsStream("foo2.txt.gz"));
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("bar\nbar2\nbar3"));
    }

    @Test
    public void contentStreamForGZIPEmbeddedConcatContentManyLines() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("gz:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/something.txt"));
        InputStream inputStream = factory.create(getClass().getResourceAsStream("foo4.txt.gz"));
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is(
                "bar\n" +
                        "bar2\n" +
                        "bar3bar\n" +
                        "bar2\n" +
                        "bar3"));
    }

    @Test
    public void contentStreamForDoubleEmbeddedContent() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("zip:zip:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/level2.zip!/level2.txt"));
        InputStream inputStream = factory.create(getZipArchiveStream());
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("https://example.org"));
    }

    @Test
    public void contentStreamForPdfPage() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("pdf:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/p3"));
        InputStream inputStream = factory.create(getPDFStream());

        ByteArrayOutputStream actual = new ByteArrayOutputStream();
        IOUtils.copy(inputStream, actual);

        InputStream expectedIs = getClass().getResourceAsStream("/bio/guoda/preston/process/elliott2023-page3_b.pdf");
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        IOUtils.copy(expectedIs, expected);

        assertThat(actual.toString(), Is.is(expected.toString()));
    }

    @Ignore
    @Test
    public void contentStreamForPdfPageNonNumeric() throws IOException {
        String contentId = "hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("pdf:" + contentId + "!/pIV"));
        InputStream inputStream = factory.create(getPDFStreamKoopman());

        ByteArrayOutputStream actual = new ByteArrayOutputStream();
        IOUtils.copy(inputStream, actual);

        InputStream expectedIs = getClass().getResourceAsStream("/bio/guoda/preston/process/koopman1994-pageIV_b.pdf");
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        IOUtils.copy(expectedIs, expected);

        assertThat(actual.toString(), Is.is(expected.toString()));
    }

    @Test(expected = IOException.class)
    public void contentStreamForNonExistingEmbeddedContent() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("zip:zip:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/level2.zip!/noSuchFile.txt"));
        InputStream inputStream = factory.create(getZipArchiveStream());
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("https://example.org"));
    }

    @Test
    public void contentStreamForLine() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("line:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/L3"));
        InputStream inputStream = factory.create(getClass().getResourceAsStream("/bio/guoda/preston/process/bhl_item.txt"));
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("49\t23\t6027003\tmobot31753000028362\ti11506039\tQK495.F67 T7 1916\t\thttps://www.biodiversitylibrary.org/item/49 \t\t1916\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00"));
    }

    @Test
    public void contentStreamForCutOfLine() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("cut:line:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/L3!/b115-122"));
        InputStream inputStream = factory.create(getClass().getResourceAsStream("/bio/guoda/preston/process/bhl_item.txt"));
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("Missouri"));
    }

    @Test
    public void contentStreamForLineRange() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("line:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/L3-L5"));
        InputStream inputStream = factory.create(new GZIPInputStream(getClass().getResourceAsStream("/bio/guoda/preston/process/bhl_item.txt.gz")));
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("49\t23\t6027003\tmobot31753000028362\ti11506039\tQK495.F67 T7 1916\t\thttps://www.biodiversitylibrary.org/item/49 \t\t1916\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n150\t47\t6208526\tmobot31753000049772\ti11534485\tQK400.5 .B65 1837\t\thttps://www.biodiversitylibrary.org/item/150 \t\t1837\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n668\t60\t197749\tmobot31753002220777\ti11576042\tQK1 .B55\tv.9 (1888)\thttps://www.biodiversitylibrary.org/item/668 \t\t1888\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n"));
    }

    @Test
    public void contentStreamForLineRanges() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("line:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/L3-L5,L7-L8"));
        InputStream inputStream = factory.create(new GZIPInputStream(getClass().getResourceAsStream("/bio/guoda/preston/process/bhl_item.txt.gz")));
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("49\t23\t6027003\tmobot31753000028362\ti11506039\tQK495.F67 T7 1916\t\thttps://www.biodiversitylibrary.org/item/49 \t\t1916\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n150\t47\t6208526\tmobot31753000049772\ti11534485\tQK400.5 .B65 1837\t\thttps://www.biodiversitylibrary.org/item/150 \t\t1837\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n668\t60\t197749\tmobot31753002220777\ti11576042\tQK1 .B55\tv.9 (1888)\thttps://www.biodiversitylibrary.org/item/668 \t\t1888\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n693\t60\t211920\tmobot31753002220991\ti11576273\tQK1 .B55\tv.31 (1902)\thttps://www.biodiversitylibrary.org/item/693 \t\t1902\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n709\t60\t218036\tmobot31753002221080\ti11576406\tQK1 .B55\tv.47 (1912)\thttps://www.biodiversitylibrary.org/item/709 \t\t1912\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n"));
    }


    @Test
    public void contentStreamForLineRangesNonExistingResource() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("line:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/L3-L5,L7-L8"));
        InputStream inputStream = factory.create(new GZIPInputStream(getClass().getResourceAsStream("/bio/guoda/preston/process/bhl_item.txt.gz")));
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is("49\t23\t6027003\tmobot31753000028362\ti11506039\tQK495.F67 T7 1916\t\thttps://www.biodiversitylibrary.org/item/49 \t\t1916\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n150\t47\t6208526\tmobot31753000049772\ti11534485\tQK400.5 .B65 1837\t\thttps://www.biodiversitylibrary.org/item/150 \t\t1837\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n668\t60\t197749\tmobot31753002220777\ti11576042\tQK1 .B55\tv.9 (1888)\thttps://www.biodiversitylibrary.org/item/668 \t\t1888\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n693\t60\t211920\tmobot31753002220991\ti11576273\tQK1 .B55\tv.31 (1902)\thttps://www.biodiversitylibrary.org/item/693 \t\t1902\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n709\t60\t218036\tmobot31753002221080\ti11576406\tQK1 .B55\tv.47 (1912)\thttps://www.biodiversitylibrary.org/item/709 \t\t1912\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n"));
    }

    @Test(expected = IOException.class)
    public void copyContentStreamForLineRanges() throws IOException {
        ContentStreamFactory factory
                = new ContentStreamFactory(
                RefNodeFactory.toIRI("line:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/L3-L5,L7-L8")
        );
        InputStream inputStream = factory.create(null);

        ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
        IOUtils.copyLarge(inputStream, out);

        assertThat(out.toString("UTF-8"), Is.is("49\t23\t6027003\tmobot31753000028362\ti11506039\tQK495.F67 T7 1916\t\thttps://www.biodiversitylibrary.org/item/49 \t\t1916\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n150\t47\t6208526\tmobot31753000049772\ti11534485\tQK400.5 .B65 1837\t\thttps://www.biodiversitylibrary.org/item/150 \t\t1837\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n668\t60\t197749\tmobot31753002220777\ti11576042\tQK1 .B55\tv.9 (1888)\thttps://www.biodiversitylibrary.org/item/668 \t\t1888\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n693\t60\t211920\tmobot31753002220991\ti11576273\tQK1 .B55\tv.31 (1902)\thttps://www.biodiversitylibrary.org/item/693 \t\t1902\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\n709\t60\t218036\tmobot31753002221080\ti11576406\tQK1 .B55\tv.47 (1912)\thttps://www.biodiversitylibrary.org/item/709 \t\t1912\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00"));
    }

    @Test
    public void copyContentStreamForCutZipHashContentId() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("cut:zip:hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f!/treatments-xml-main/data/C3/05/87/C30587A9A562FF93FF2F350F875ED55F.xml!/b3714-3735"));
        InputStream inputStream = factory.create(getClass().getResourceAsStream("treatments-xml-issue205.zip"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyLarge(inputStream, out);

        assertThat(out.toString("UTF-8"), Is.is("Barbastello leucomelas"));
    }


    @Test
    public void createThumbnailFromImage() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("thumbnail:hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f"));
        InputStream inputStream = factory.create(getClass().getResourceAsStream("BRIT67501.jpg"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyLarge(inputStream, out);

        BufferedImage actualThumbnail = ImageIO.read(new ByteArrayInputStream(out.toByteArray()));

        assertNotNull(actualThumbnail);

        assertThat(actualThumbnail.getHeight(), Is.is(256));
        assertThat(actualThumbnail.getWidth(), Is.is(171));
    }

    @Test
    public void createThumbnailFromTIFFImage() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("thumbnail:hash://sha256/75812248fb10b8254890141752ae21341af94a8a7bedeeb0b0dd0a58a304c201"));
        InputStream inputStream = factory.create(getClass().getResourceAsStream("Peponapis-pruinosa-UCSB-IZC00040452.tif"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyLarge(inputStream, out);

        BufferedImage actualThumbnail = ImageIO.read(new ByteArrayInputStream(out.toByteArray()));

        assertNotNull(actualThumbnail);

        assertThat(actualThumbnail.getHeight(), Is.is(205));
        assertThat(actualThumbnail.getWidth(), Is.is(256));
    }

    @Test(expected = IOException.class)
    public void createThumbnailFromNonImage() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("thumbnail:hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f"));
        try {
            InputStream inputStream = factory.create(getClass().getResourceAsStream("treatments-xml-issue205.zip"));
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IOUtils.copyLarge(inputStream, out);
        } catch (IOException ex) {
            assertThat(ex.getMessage(), Is.is("failed to create inputstream for [hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f]"));
            throw ex;
        }

    }

    @Test
    public void lineNumberStream() {
        LongStream streamOfNumbers = ContentStreamFactory.getLineNumberStream(
                RefNodeFactory.toIRI("hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f"),
                RangeType.Line,
                RefNodeFactory.toIRI("line:hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f!/L1,L3"));

        long[] numbers = streamOfNumbers.toArray();
        assertThat(numbers.length, Is.is(2));
        assertThat(numbers[0], Is.is(1L));
        assertThat(numbers[1], Is.is(3L));
    }


    @Test
    public void lineNumberStreamRange() {
        LongStream streamOfNumbers = ContentStreamFactory.getLineNumberStream(
                RefNodeFactory.toIRI("hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f"),
                RangeType.Line,
                RefNodeFactory.toIRI("line:hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f!/L1-L3"));

        long[] numbers = streamOfNumbers.toArray();
        assertThat(numbers.length, Is.is(3));
        assertThat(numbers[0], Is.is(1L));
        assertThat(numbers[1], Is.is(2L));
        assertThat(numbers[2], Is.is(3L));
    }

    @Test
    public void lineNumberStreamRangeAndIntersection() {
        LongStream streamOfNumbers = ContentStreamFactory.getLineNumberStream(
                RefNodeFactory.toIRI("hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f"),
                RangeType.Line,
                RefNodeFactory.toIRI("line:hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f!/L1-L3,L5"));

        long[] numbers = streamOfNumbers.toArray();
        assertThat(numbers.length, Is.is(4));
        assertThat(numbers[0], Is.is(1L));
        assertThat(numbers[1], Is.is(2L));
        assertThat(numbers[2], Is.is(3L));
        assertThat(numbers[3], Is.is(5L));
    }

    @Test
    public void lineNumberStreamRangeAndIntersectionRange() {
        LongStream streamOfNumbers = ContentStreamFactory.getLineNumberStream(
                RefNodeFactory.toIRI("hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f"),
                RangeType.Line,
                RefNodeFactory.toIRI("line:hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f!/L1-L3,L5-L7"));

        long[] numbers = streamOfNumbers.toArray();
        assertThat(numbers.length, Is.is(6));
        assertThat(numbers[0], Is.is(1L));
        assertThat(numbers[1], Is.is(2L));
        assertThat(numbers[2], Is.is(3L));
        assertThat(numbers[3], Is.is(5L));
        assertThat(numbers[4], Is.is(6L));
        assertThat(numbers[5], Is.is(7L));
    }

    @Test
    public void pageNumberStreamRangeAndIntersectionRangeShorthand() {
        LongStream streamOfNumbers = ContentStreamFactory.getLineNumberStream(
                RefNodeFactory.toIRI("hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f"),
                RangeType.Page,
                RefNodeFactory.toIRI("pdf:hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f!/p1-p3,p5-p7"));

        long[] numbers = streamOfNumbers.toArray();
        assertThat(numbers.length, Is.is(6));
        assertThat(numbers[0], Is.is(1L));
        assertThat(numbers[1], Is.is(2L));
        assertThat(numbers[2], Is.is(3L));
        assertThat(numbers[3], Is.is(5L));
        assertThat(numbers[4], Is.is(6L));
        assertThat(numbers[5], Is.is(7L));
    }

    @Test
    public void pageNumberStreamRangeSingle() {
        LongStream streamOfNumbers = ContentStreamFactory.getLineNumberStream(
                RefNodeFactory.toIRI("hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f"),
                RangeType.Page,
                RefNodeFactory.toIRI("pdf:hash://sha256/d89ad03a0c058ecb19c49d158ea1324b83669713a9d446e49786bdfcc23a3c3f!/p7"));

        long[] numbers = streamOfNumbers.toArray();
        assertThat(numbers.length, Is.is(1));
        assertThat(numbers[0], Is.is(7L));
    }
}