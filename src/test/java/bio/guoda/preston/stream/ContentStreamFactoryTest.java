package bio.guoda.preston.stream;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.cxf.helpers.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

public class ContentStreamFactoryTest {

    @Test(expected = IllegalArgumentException.class)
    public void noContentStreamForNonContentHash() throws IOException {
        new ContentStreamFactory(RefNodeFactory.toIRI("foo:bar"));
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

    @Test
    public void contentStreamForEmbeddedContent() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("zip:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/level1.txt"));
        InputStream inputStream = factory.create(getZipArchiveStream());
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8.name()), Is.is("https://example.org"));
    }

    @Test
    public void contentStreamForCutEmbeddedContent() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("cut:zip:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/level1.txt!/b1-5"));
        InputStream inputStream = factory.create(getZipArchiveStream());
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8.name()), Is.is("https"));
    }

    @Test
    public void contentStreamForDoubleEmbeddedContent() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("zip:zip:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/level2.zip!/level2.txt"));
        InputStream inputStream = factory.create(getZipArchiveStream());
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8.name()), Is.is("https://example.org"));
    }

    @Test(expected = IOException.class)
    public void contentStreamForNonExistingEmbeddedContent() throws IOException {
        ContentStreamFactory factory = new ContentStreamFactory(RefNodeFactory.toIRI("zip:zip:hash://sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!/level2.zip!/noSuchFile.txt"));
        InputStream inputStream = factory.create(getZipArchiveStream());
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8.name()), Is.is("https://example.org"));
    }


}