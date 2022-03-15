package org.gbif.dwc;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.gbif.dwc.meta.DwcMetaFiles;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class DwCArchiveCitationStreamHandler implements ContentStreamHandler {
    private static final SAXParserFactory SAX_FACTORY = SAXParserFactory.newInstance();

    public static final String META_XML = "meta.xml";
    private final Dereferencer<InputStream> dereferencer;
    private ContentStreamHandler contentStreamHandler;

    public DwCArchiveCitationStreamHandler(ContentStreamHandler contentStreamHandler, Dereferencer<InputStream> inputStreamDereferencer) {
        this.contentStreamHandler = contentStreamHandler;
        this.dereferencer = inputStreamDereferencer;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        String iriString = version.getIRIString();
        try {
            if (StringUtils.endsWith(iriString, "/" + META_XML)) {
                Archive starRecords = DwcMetaFiles.fromMetaDescriptor(is);

                String metadataLocation = starRecords.getMetadataLocation();

                if (StringUtils.isNotBlank(metadataLocation)
                        && !StringUtils.startsWith(metadataLocation, "/")
                        && !StringUtils.startsWith(metadataLocation, "http://")
                        && !StringUtils.startsWith(metadataLocation, "https://")) {
                    String prefix = StringUtils.substring(iriString, 0, iriString.length() - META_XML.length());

                    String metaDataIRI = prefix + metadataLocation;
                    InputStream emlStream = dereferencer.get(RefNodeFactory.toIRI(metaDataIRI));

                    if (emlStream != null) {
                        SAXParser p = SAX_FACTORY.newSAXParser();
                        p.parse(emlStream, new CitationSaxHandler(metaDataIRI));
                    }
                }

                return true;
            }
        } catch (IOException | SAXException | ParserConfigurationException e) {
            throw new ContentStreamException("failed to parse [" + iriString + "]", e);
        }
        return false;
    }

    @Override
    public boolean shouldKeepReading() {
        return contentStreamHandler.shouldKeepReading();
    }


    private static class CitationSaxHandler extends SimpleSaxHandler {

        AtomicBoolean inCitation = new AtomicBoolean(false);

        AtomicReference<StringBuilder> builder = new AtomicReference<>();

        private String namespace;


        public CitationSaxHandler(String metaDataIRI) {
            this.namespace = metaDataIRI;
        }

        @Override
        public void characters(char[] ch, int start, int length) {
            if (inCitation.get()) {
                builder.get().append(StringUtils.replace(new String(ch, start, length), "\n", ""));
            }
            super.characters(ch, start, length);
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            if (StringUtils.equals(qName, "citation")) {
                builder.set(new StringBuilder());
                inCitation.set(true);
            }
            super.startElement(uri, localName, qName, attributes);
        }

        public void endElement(String uri, String localName, String qName) throws SAXException {
            if (StringUtils.equals(qName, "citation")) {
                inCitation.set(false);
                String citationString = StringUtils.trim(builder.get().toString());
                if (StringUtils.isNoneBlank(citationString)) {
                    System.out.println
                            (StringUtils.removeEnd(citationString, ".") + ". Accessed at <" + namespace + "> .");
                }
            }
            super.endElement(uri, localName, qName);
        }
    }
}
