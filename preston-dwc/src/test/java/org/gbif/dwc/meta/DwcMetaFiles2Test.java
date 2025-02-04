package org.gbif.dwc.meta;

import org.gbif.dwc.Archive;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertNotNull;

public class DwcMetaFiles2Test {

    @Test
    public void checkSchema() throws IOException, SAXException {
        // see https://github.com/bio-guoda/preston/issues/319
        InputStream resourceAsStream = getClass().getResourceAsStream("issue319/meta.xml");
        assertNotNull(resourceAsStream);
        Archive starRecords = DwcMetaFiles2.fromMetaDescriptor(resourceAsStream);

        assertThat(starRecords.getCore(), is(notNullValue()));
        assertThat(starRecords.getExtensions().size(), is(greaterThan(0)));


    }

}