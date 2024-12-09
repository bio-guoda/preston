package bio.guoda.preston.stream;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.io.IOUtils;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;

public class ArchiveStreamHandlerTest {

    @Test
    public void textDetectedAsCPIO() throws ArchiveException {
        // https://github.com/bio-guoda/preston/issues/200
        // the uuid of a record number appears to align with the magic signature of an old CPIO archive
        // text extracted from
        // preston cat --remote https://linker.bio  'line:zip:hash://sha256/9af95ab26a1886db0f17b2102838a9b898a1627872590afb485044cb25d2a5c6!/name.tsv!/L185815'

        String detect = ArchiveStreamFactory.detect(IOUtils.toInputStream(
                "07070776-e643-47bf-afab-22da04e3fd9c\tLasioglossum (Chilalictus) nefrens Walker, 1995\tWalker\tspecies\t\tLasioglossum\tChilalictus\tnefrens\t\tICZN\testablished\t\t1995\t\n", StandardCharsets.UTF_8));
        MatcherAssert.assertThat(detect, is("cpio"));
    }

    @Test
    public void textDetectedAsCPIO2() throws ArchiveException {
        // the uuid of a record number appears to align with the magic signature of an old CPIO archive
        String detect = ArchiveStreamFactory.detect(IOUtils.toInputStream(
                "070707", StandardCharsets.UTF_8));
        MatcherAssert.assertThat(detect, is("cpio"));
    }

    @Test(expected = ArchiveException.class)
    public void textNotDetectedAsCPIO3() throws ArchiveException {
        ArchiveStreamFactory.detect(IOUtils.toInputStream(
                "07070", StandardCharsets.UTF_8));
    }

}