package org.gbif.dwc;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.gbif.dwc.meta.DwcMetaFiles;
import org.gbif.dwc.meta.DwcMetaFiles2;
import org.hamcrest.core.Is;
import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class DwCArchiveStreamHandlerTest {

    @Ignore("this test fails because of issue documented in https://github.com/bio-guoda/preston/issues/161")
    @Test
    public void handleEscapedTerminationCharacters() throws SAXException, IOException, ContentStreamException {
        // from https://github.com/bio-guoda/preston/issues/161
        InputStream is = getClass().getResourceAsStream("issue161/meta.xml");

        Archive starRecords = DwcMetaFiles.fromMetaDescriptor(is);

        assertThat(starRecords.getCore().getFieldsTerminatedBy(), Is.is("\t"));
        assertThat(starRecords.getCore().getLinesTerminatedBy(), Is.is("\n"));

    }

    @Test
    public void handleEscapedTerminationCharactersPatched() throws SAXException, IOException, ContentStreamException {
        // from https://github.com/bio-guoda/preston/issues/161
        InputStream is = getClass().getResourceAsStream("issue161/meta.xml");

        Archive starRecords = DwcMetaFiles2.fromMetaDescriptor(is);

        assertThat(starRecords.getCore().getFieldsTerminatedBy(), Is.is("\t"));
        assertThat(starRecords.getCore().getLinesTerminatedBy(), Is.is("\n"));

    }

    @Test(expected = ContentStreamException.class)
    public void unescapedDoubleQuotesInCSV() throws ContentStreamException {
        // from https://github.com/bio-guoda/preston/issues/162
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        DwCArchiveStreamHandler handler = new DwCArchiveStreamHandler(new ContentStreamHandler() {
            @Override
            public boolean handle(IRI version, InputStream in) throws ContentStreamException {
                return false;
            }

            @Override
            public boolean shouldKeepProcessing() {
                return false;
            }
        }, new Dereferencer<InputStream>() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                String resourceNAme = StringUtils.replace(uri.getIRIString(), "foo:bar!", "issue162");
                return DwCArchiveStreamHandlerTest.this.getClass()
                        .getResourceAsStream(resourceNAme);
            }
        }, os);

        try {
            handler.handle(RefNodeFactory.toIRI("foo:bar!/meta.xml"),
                    getClass().getResourceAsStream("issue162/meta.xml"));
        } catch(ContentStreamException ex) {
            assertThat(ex.getMessage(), is("failed to handle dwc records from <line:foo:bar!/DarwinCore.txt!/L2>"));
            throw ex;
        }
    }

}