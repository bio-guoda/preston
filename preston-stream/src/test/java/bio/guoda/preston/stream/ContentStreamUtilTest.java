package bio.guoda.preston.stream;

import org.apache.commons.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ContentStreamUtilTest {

    @Test
    public void cutBytes() throws IOException {
        InputStream inBytes = IOUtils.toInputStream("0123456789", StandardCharsets.UTF_8);
        InputStream outBytes = ContentStreamUtil.cutBytes(inBytes, 4, 7);

        assertThat(IOUtils.toString(outBytes, StandardCharsets.UTF_8), is("456"));
    }

    @Test(expected = IOException.class)
    public void cutBadRange() throws IOException {
        InputStream inBytes = IOUtils.toInputStream("0123456789", StandardCharsets.UTF_8);
        ContentStreamUtil.cutBytes(inBytes, 14, 17);
    }

    @Test
    public void truncateVSFGZIPNotation() {
        String contentId = "hash://sha256/bf18509ad6a2a97143d4f74e72dc4177ec31a4c50b3d7052f9a9cf6735f65e43";
        String query = "tar:gz:" + contentId + "!/50418.1.1.tar!/0050418/1.1/data/0-data/NODC_TaxonomicCode_V8_CD-ROM/TAXBRIEF.DAT";
        String truncated = ContentStreamUtil.truncateGZNotationForVFSIfNeeded(query);
        assertThat(truncated, is("tar:gz:" + contentId + "!/0050418/1.1/data/0-data/NODC_TaxonomicCode_V8_CD-ROM/TAXBRIEF.DAT"));
    }

    @Test
    public void truncateVSFGZIPNotation2() {
        String contentId = "hash://sha256/bf18509ad6a2a97143d4f74e72dc4177ec31a4c50b3d7052f9a9cf6735f65e43";
        String query = "tar:gz:" + contentId + "!/foo.tar!/0050418/1.1/data/0-data/NODC_TaxonomicCode_V8_CD-ROM/TAXBRIEF.DAT";
        String truncated = ContentStreamUtil.truncateGZNotationForVFSIfNeeded(query);
        assertThat(truncated, is("tar:gz:" + contentId + "!/0050418/1.1/data/0-data/NODC_TaxonomicCode_V8_CD-ROM/TAXBRIEF.DAT"));
    }

    @Test
    public void truncateVSFGZIPNotation3() {
        String contentId = "hash://sha256/bf18509ad6a2a97143d4f74e72dc4177ec31a4c50b3d7052f9a9cf6735f65e43";
        String query = "tar:gz:" + contentId + "!/50418.1.1.tar!/TAXBRIEF.DAT";
        String truncated = ContentStreamUtil.truncateGZNotationForVFSIfNeeded(query);
        assertThat(truncated, is("tar:gz:" + contentId + "!/TAXBRIEF.DAT"));
    }

    @Test
    public void nonVSFGZIPNotation() {
        String query = "tar:gz:hash://sha256/bf18509ad6a2a97143d4f74e72dc4177ec31a4c50b3d7052f9a9cf6735f65e43!/0050418/1.1/data/0-data/NODC_TaxonomicCode_V8_CD-ROM/TAXBRIEF.DAT";
        String truncated = ContentStreamUtil.truncateGZNotationForVFSIfNeeded(query);
        assertThat(truncated, is(query));
    }

    @Test
    public void apacheVFSUrl() {
        String input = "tar:gz:hash://sha256/bababab!/nested.tar!/file.txt";
        assertThat(ContentStreamUtil.truncateGZNotationForVFSIfNeeded(input),
                Is.is("tar:gz:hash://sha256/bababab!/file.txt"));

    }

    @Test
    public void apacheVFSUrl2() {
        String input = "zip:tar:gz:hash://sha256/bababab!/nested.tar!/file.zip!/file.txt";
        assertThat(ContentStreamUtil.truncateGZNotationForVFSIfNeeded(input),
                Is.is("zip:tar:gz:hash://sha256/bababab!/file.zip!/file.txt"));

    }

    @Test
    public void apacheVFSUrl3() {
        String url = "zip:tar:gz:hash://sha256/bababab!/file.zip!/file.txt";
        url = ContentStreamUtil.truncateGZNotationForVFSIfNeeded(url);

        assertThat(url, Is.is("zip:tar:gz:hash://sha256/bababab!/file.zip!/file.txt"));

    }

    @Test
    public void tarGzVSF() {
        String url = "tar:gz:hash://sha256/bedcc1f122d59ec002e0e6d2802c0e422eadf6208669fff141a895bd3ed15d4a!/FaEu-DWCA/eml.xml";
        String s = ContentStreamUtil.truncateGZNotationForVFSIfNeeded("tar:gz:hash://sha256/bedcc1f122d59ec002e0e6d2802c0e422eadf6208669fff141a895bd3ed15d4a!/FaEu-DWCA/eml.xml");

        assertThat(s, Is.is(url));
    }


}