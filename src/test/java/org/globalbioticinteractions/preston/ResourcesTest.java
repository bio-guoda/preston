package org.globalbioticinteractions.preston;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class ResourcesTest {

    @Test
    public void irelandServerPickyAboutContentHeaderWithJSON() throws IOException {
        InputStream is = Resources.asInputStream(URI.create("http://gbif.biodiversityireland.ie/HareSurveyOfIreland0607.zip"));
        IOUtils.copy(is, new NullOutputStream());
    }

}