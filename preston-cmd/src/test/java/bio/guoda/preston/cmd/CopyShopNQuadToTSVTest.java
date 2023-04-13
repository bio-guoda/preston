package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorStateAlwaysContinue;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;

public class CopyShopNQuadToTSVTest {

    @Test
    public void copyWithIRIObject() throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        new CopyShopNQuadToTSV(new ProcessorStateAlwaysContinue())
                .copy(IOUtils.toInputStream(exampleWithIRIObject(), StandardCharsets.UTF_8),
                        os
                );

        assertThat(new String(os.toByteArray(), StandardCharsets.UTF_8),
                Is.is("https://preston.guoda.bio\thttp://www.w3.org/1999/02/22-rdf-syntax-ns#type\thttp://www.w3.org/ns/prov#SoftwareAgent\td2c8a96a-89c8-4dd6-ba37-06809d4ff9ae\n"));
    }


    @Test
    public void withRelativeIRIGraphLabel() {
        Pattern pattern = CopyShopNQuadToTSV.WITH_IRI_OBJECT;

        Matcher matcher = pattern.matcher(
                exampleWithIRIObject()
        );

        assertThat(matcher.matches(), Is.is(true));

        assertThat(matcher.group("subject"), Is.is("https://preston.guoda.bio"));
        assertThat(matcher.group("verb"), Is.is("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"));
        assertThat(matcher.group("object"), Is.is("http://www.w3.org/ns/prov#SoftwareAgent"));
        assertThat(matcher.group("namespace"), Is.is("d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae"));
    }

    public static String exampleWithIRIObject() {
        return "<https://preston.guoda.bio>" +
                " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" +
                " <http://www.w3.org/ns/prov#SoftwareAgent>" +
                " <d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae> .";
    }

    @Test
    public void withoutGraphLabel() {
        // only statements with graph labels are supported
        Pattern pattern = CopyShopNQuadToTSV.WITH_IRI_OBJECT;

        Matcher matcher = pattern.matcher(
                "<https://preston.guoda.bio>" +
                        " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" +
                        " <http://www.w3.org/ns/prov#SoftwareAgent>" +
                        " ."
        );

        assertThat(matcher.matches(), Is.is(false));
    }

    @Test
    public void withBlanks() {
        // blanks not supported for now
        Pattern pattern = CopyShopNQuadToTSV.WITH_IRI_OBJECT;

        Matcher matcher = pattern.matcher(
                "_:foo" +
                        " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" +
                        " <http://www.w3.org/ns/prov#SoftwareAgent>" +
                        " ."
        );

        assertThat(matcher.matches(), Is.is(false));
    }


    @Test
    public void simpleRDFParserLiteralWithTypeIRI() {
        Pattern pattern = CopyShopNQuadToTSV.WITH_LITERAL_OBJECT;

        Matcher matcher = pattern.matcher(
                getLiteralObjectWithTypeAndRelativeGraphLabelIRI());

        assertThat(matcher.matches(), Is.is(true));

        assertThat(matcher.group("subject"), Is.is("https://preston.guoda.bio"));
        assertThat(matcher.group("verb"), Is.is("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"));
        assertThat(matcher.group("object"), Is.is("\"2020-09-12T05:57:53.420Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"));
        assertThat(matcher.group("namespace"), Is.is("d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae"));
    }

    public static String getLiteralObjectWithTypeAndRelativeGraphLabelIRI() {
        return "<https://preston.guoda.bio>" +
                " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" +
                " \"2020-09-12T05:57:53.420Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>" +
                " <d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae> .";
    }

    @Test
    public void simpleRDFParserLiteralWithTypeIRINoGraphLabel() {
        Pattern pattern = CopyShopNQuadToTSV.WITH_LITERAL_OBJECT;

        Matcher matcher = pattern.matcher(
                "<https://preston.guoda.bio>" +
                        " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" +
                        " \"2020-09-12T05:57:53.420Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>" +
                        " .");

        assertThat(matcher.matches(), Is.is(false));
    }

    @Test
    public void simpleRDFParserLiteralWithLanguageTag() {
        Pattern pattern = CopyShopNQuadToTSV.WITH_LITERAL_OBJECT;

        Matcher matcher = pattern.matcher(
                "<https://preston.guoda.bio>" +
                        " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" +
                        " \"duck\"@en" +
                        " <d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae> ."
        );

        assertThat(matcher.matches(), Is.is(true));

        assertThat(matcher.group("subject"), Is.is("https://preston.guoda.bio"));
        assertThat(matcher.group("verb"), Is.is("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"));
        assertThat(matcher.group("object"), Is.is("\"duck\"@en"));
        assertThat(matcher.group("namespace"), Is.is("d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae"));
    }

    @Test
    public void simpleRDFParserLiteralWithoutLanguageTag() {
        Pattern pattern = CopyShopNQuadToTSV.WITH_LITERAL_OBJECT;

        Matcher matcher = pattern.matcher(
                "<https://preston.guoda.bio>" +
                        " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" +
                        " \"duck\"" +
                        " <d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae> ."
        );

        assertThat(matcher.matches(), Is.is(true));

        assertThat(matcher.group("subject"), Is.is("https://preston.guoda.bio"));
        assertThat(matcher.group("verb"), Is.is("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"));
        assertThat(matcher.group("object"), Is.is("\"duck\""));
        assertThat(matcher.group("namespace"), Is.is("d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae"));
    }


}