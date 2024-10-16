package org.gbif.dwc;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class CitationSaxHandler extends SimpleSaxHandler {

    private final OutputStream os;
    AtomicBoolean inCitation = new AtomicBoolean(false);
    AtomicBoolean inBibliography = new AtomicBoolean(false);
    AtomicBoolean inTitle = new AtomicBoolean(false);

    StringBuilder builder = new StringBuilder();
    StringBuilder titleBuilder = new StringBuilder();

    private String namespace;


    public CitationSaxHandler(String metaDataIRI, OutputStream os) {
        this.namespace = metaDataIRI;
        this.os = os;
    }

    @Override
    public void characters(char[] ch, int start, int length) {
        if (inCitation.get() && !inBibliography.get()) {
            append(ch, start, length, builder);
        }
        if (inTitle.get()) {
            append(ch, start, length, titleBuilder);
        }
        super.characters(ch, start, length);
    }

    private StringBuilder append(char[] ch, int start, int length, StringBuilder builder) {
        return builder.append(StringUtils.trim(StringUtils.replace(new String(ch, start, length), "\n", "")));
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if (StringUtils.equals(qName, "citation")) {
            inCitation.set(true);
        }
        if (StringUtils.equals(qName, "bibliography")) {
            inBibliography.set(true);
        }

        if (StringUtils.equals(qName, "title")) {
            inTitle.set(true);
        }
        super.startElement(uri, localName, qName, attributes);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (StringUtils.equals(qName, "citation")) {
            inCitation.set(false);
        }
        if (StringUtils.equals(qName, "bibliography")) {
            inBibliography.set(false);
        }
        if (StringUtils.equals(qName, "title")) {
            inTitle.set(false);
        }

        super.endElement(uri, localName, qName);
    }

    @Override
    public void endDocument() {
        String citationString = builder.toString();
        citationString = StringUtils.isBlank(citationString)
                ? titleBuilder.toString()
                : citationString;

        String emptyOrPunctuated = StringUtils.isBlank(citationString) ? "" : StringUtils.removeEnd(citationString, ".") + ". ";
        try {
            IOUtils.write(emptyOrPunctuated + "Accessed at <" + namespace + "> .\n", os, StandardCharsets.UTF_8);
        } catch (IOException e) {
            //
        }
    }
}
