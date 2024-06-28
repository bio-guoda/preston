package bio.guoda.preston.cmd;

import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.regex.Matcher;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;


public class DarkTaxonFileStreamHandlerTest {

    @Test
    public void parseReadme() {
        Matcher matcher = DarkTaxonFileStreamHandler.HASH_AND_FILEPATH_PATTERN
                .matcher("72a63d47805f78e4529ec282e3e8e8412beb456e571c1e2276a107b3f0fa9822  BMT121/BMT0009397/BMT121_BMT0009397_RAW_Data_01/BMT121_BMT0009397_RAW_01_01.tiff");

        assertTrue(matcher.matches());

        assertThat(matcher.group("sha256hash"), Is.is("72a63d47805f78e4529ec282e3e8e8412beb456e571c1e2276a107b3f0fa9822"));
        assertThat(matcher.group("filepath"), Is.is("BMT121/BMT0009397/BMT121_BMT0009397_RAW_Data_01/BMT121_BMT0009397_RAW_01_01.tiff"));
    }

    @Test
    public void parseRawImageFilename() {
        String filepath = "BMT121/BMT0009397/BMT121_BMT0009397_RAW_Data_01/BMT121_BMT0009397_RAW_01_01.tiff";
        Matcher matcher = DarkTaxonFileStreamHandler.RAW_IMAGEFILE_PATTERN.matcher(filepath);
        assertThat(matcher.matches(), Is.is(true));
        assertThat(matcher.group("plateId"), Is.is("BMT121"));
        assertThat(matcher.group("specimenId"), Is.is("BMT0009397"));
        assertThat(matcher.group("imageStackNumber"), Is.is("01"));
        assertThat(matcher.group("imageNumber"), Is.is("01"));
        assertThat(matcher.group("extension"), Is.is("tiff"));


    }

    @Test
    public void parseStackedImageFilename() {
        String filepath = "BMT121/BMT0009397/BMT121_BMT0009397_stacked_01.tiff";
        Matcher matcher = DarkTaxonFileStreamHandler.STACKED_IMAGEFILE_PATTERN.matcher(filepath);
        assertThat(matcher.matches(), Is.is(true));
        assertThat(matcher.group("plateId"), Is.is("BMT121"));
        assertThat(matcher.group("specimenId"), Is.is("BMT0009397"));
        assertThat(matcher.group("imageStackNumber"), Is.is("01"));
        assertThat(matcher.group("extension"), Is.is("tiff"));
    }

}