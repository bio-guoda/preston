package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.apache.tika.Tika;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class TextExtractUtilTest {

    @Test
    public void extractTextFromDwCA() throws IOException {
        Tika tika = new Tika();
        try (InputStream resourceAsStream = getClass().getResourceAsStream("/bio/guoda/preston/plazidwca.zip")) {
            assertNotNull(resourceAsStream);
            Reader reader = tika.parse(resourceAsStream);
            readerOutputContainsString(reader, "Phylogeny of Calyptraeotheres");
        }
    }

    @Test
    public void extractTextFromTarGz() throws IOException {
        Tika tika = new Tika();
        try (InputStream resourceAsStream = getClass().getResourceAsStream("/preston-1a.tar.gz")) {
            assertNotNull(resourceAsStream);
            Reader reader = tika.parse(resourceAsStream);
            readerOutputContainsString(reader, "Constitution of the Human Brain");
        }
    }

    public void readerOutputContainsString(Reader reader, String someString) throws IOException {
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        IOUtils.copy(reader, boas, StandardCharsets.UTF_8);
        assertThat(IOUtils.toString(boas.toByteArray(), StandardCharsets.UTF_8.name()),
                containsString(someString));
    }

}
