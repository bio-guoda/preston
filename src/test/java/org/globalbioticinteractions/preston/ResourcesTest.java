package org.globalbioticinteractions.preston;

import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;

public class ResourcesTest {

    @Test
    public void ireland() throws IOException {
        Resources.asInputStream(URI.create("http://gbif.biodiversityireland.ie/HareSurveyOfIreland0607.zip"));
    }

}