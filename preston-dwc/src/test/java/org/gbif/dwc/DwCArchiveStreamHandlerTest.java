package org.gbif.dwc;

import bio.guoda.preston.stream.ContentStreamException;
import org.gbif.dwc.meta.DwcMetaFiles;
import org.gbif.dwc.meta.DwcMetaFiles2;
import org.hamcrest.core.Is;
import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.MatcherAssert.assertThat;

public class DwCArchiveStreamHandlerTest {

    @Ignore("this test fails")
    @Test
    public void escapedTerminationCharactersInMetaXML() throws SAXException, IOException, ContentStreamException {
        // from https://github.com/bio-guoda/preston/issues/161
        InputStream is = getClass().getResourceAsStream("issue161/meta.xml");

        Archive starRecords = DwcMetaFiles.fromMetaDescriptor(is);

        assertThat(starRecords.getCore().getFieldsTerminatedBy(), Is.is("\t"));
        assertThat(starRecords.getCore().getLinesTerminatedBy(), Is.is("\n"));

    }

    @Test
    public void escapedFieldDelimitersInMetaXMLPatched() throws SAXException, IOException, ContentStreamException {
        // from https://github.com/bio-guoda/preston/issues/161
        InputStream is = getClass().getResourceAsStream("issue161/meta.xml");

        Archive starRecords = DwcMetaFiles2.fromMetaDescriptor(is);

        assertThat(starRecords.getCore().getFieldsTerminatedBy(), Is.is("\t"));
        assertThat(starRecords.getCore().getLinesTerminatedBy(), Is.is("\n"));

    }

    @Test
    public void unescapedDoubleQuotesInCSV() {
        // from https://github.com/bio-guoda/preston/issues/162

    }

}