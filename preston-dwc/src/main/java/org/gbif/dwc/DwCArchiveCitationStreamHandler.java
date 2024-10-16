package org.gbif.dwc;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactoryBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.gbif.dwc.meta.DwcMetaFiles2;
import org.xml.sax.SAXException;
import org.yaml.snakeyaml.Yaml;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class DwCArchiveCitationStreamHandler implements ContentStreamHandler {
    private static final SAXParserFactory SAX_FACTORY = SAXParserFactory.newInstance();

    public static final String META_XML = "meta.xml";
    private final Dereferencer<InputStream> dereferencer;
    private final CountingOutputStream os;
    private ContentStreamHandler contentStreamHandler;

    public DwCArchiveCitationStreamHandler(ContentStreamHandler contentStreamHandler,
                                           Dereferencer<InputStream> inputStreamDereferencer,
                                           OutputStream os) {
        this.contentStreamHandler = contentStreamHandler;
        this.dereferencer = inputStreamDereferencer;
        this.os = new CountingOutputStream(os);
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        String iriString = version.getIRIString();
        try {
            if (StringUtils.endsWith(iriString, "/" + META_XML)) {
                Archive starRecords = DwcMetaFiles2.fromMetaDescriptor(is);

                String metadataLocation = starRecords.getMetadataLocation();

                if (StringUtils.isNotBlank(metadataLocation)
                        && !StringUtils.startsWith(metadataLocation, "/")
                        && !StringUtils.startsWith(metadataLocation, "http://")
                        && !StringUtils.startsWith(metadataLocation, "https://")) {
                    String prefix = StringUtils.substring(iriString, 0, iriString.length() - META_XML.length());

                    String metaDataIRI = prefix + metadataLocation;
                    try (InputStream metadataStream = dereferencer.get(RefNodeFactory.toIRI(metaDataIRI))) {
                        if (StringUtils.endsWith(metaDataIRI, ".xml")) {
                            handleAsXML(metaDataIRI, metadataStream);
                        } else if (StringUtils.endsWith(metaDataIRI, ".yaml") || StringUtils.endsWith(metaDataIRI, ".yml")) {
                            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
                            JsonNode jsonNode = objectMapper.readTree(metadataStream);
                            JsonNode at = jsonNode.at("/title");
                            if (!at.isMissingNode()) {
                                IOUtils.copy(IOUtils.toInputStream(at.asText() + ". " + getAccessedAt(RefNodeFactory.toIRI(metaDataIRI)) + "\n", StandardCharsets.UTF_8), os);
                            }

                        }
                    }

                }
                if (os.getCount() == 0) {
                    IOUtils.copy(IOUtils.toInputStream(getAccessedAt(version), StandardCharsets.UTF_8), os);
                }
                return true;
            }
        } catch (IOException | SAXException | ParserConfigurationException e) {
            throw new ContentStreamException("failed to parse [" + iriString + "]", e);
        }
        return false;
    }

    private String getAccessedAt(IRI version) {
        return "Accessed at <" + version.getIRIString() + ">.";
    }

    private void handleAsXML(String metaDataIRI, InputStream emlStream) throws ParserConfigurationException, SAXException, IOException {
        if (emlStream != null) {
            SAXParser p = SAX_FACTORY.newSAXParser();
            p.parse(emlStream, new CitationSaxHandler(metaDataIRI, os));
        }
    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
