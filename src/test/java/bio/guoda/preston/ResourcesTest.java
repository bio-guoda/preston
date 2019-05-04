package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import bio.guoda.preston.model.RefNodeFactory;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class ResourcesTest {

    @Test
    public void irelandServerPickyAboutContentHeaderWithJSON() throws IOException {
        InputStream is = Resources.asInputStream(RefNodeFactory.toIRI(URI.create("http://gbif.biodiversityireland.ie/HareSurveyOfIreland0607.zip")));
        IOUtils.copy(is, new NullOutputStream());
    }


    @Test
    public void dataOneObjectLocationList() throws IOException {
        InputStream is = Resources.asInputStream(RefNodeFactory.toIRI(URI.create("https://cn.dataone.org/cn/v2/resolve/aekos.org.au%2Fcollection%2Fnsw.gov.au%2Fnsw_atlas%2Fvis_flora_module%2FJTH_BMW.20160629")), Resources.REDIRECT_CODES);

        StringWriter output = new StringWriter();
        IOUtils.copy(is, output, StandardCharsets.UTF_8);
        assertThat(output.getBuffer().toString(), CoreMatchers.containsString("objectLocationList"));
    }

}