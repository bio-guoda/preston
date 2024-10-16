package org.gbif.dwc;

import org.hamcrest.core.Is;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;


public class CitationSaxHandlerTest {
    private static final SAXParserFactory SAX_FACTORY = SAXParserFactory.newInstance();

    @Test
    public void citationTitleOnly() throws ParserConfigurationException, SAXException, IOException {

        InputStream is = getClass().getResourceAsStream("/bio/guoda/preston/iNaturalist.eml.xml");

        SAXParser p = SAX_FACTORY.newSAXParser();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        p.parse(is, new CitationSaxHandler("https://example.org", os));


        assertThat(new String(os.toByteArray(), StandardCharsets.UTF_8), Is.is("iNaturalist Research-grade Observations. Accessed at <https://example.org> .\n"));

    }

    @Test
    public void citationWithBibliography() throws ParserConfigurationException, SAXException, IOException {

        InputStream is = getClass().getResourceAsStream("/bio/guoda/preston/Ramírez-Chaves-et-al-2022.eml.xml");

        SAXParser p = SAX_FACTORY.newSAXParser();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        p.parse(is, new CitationSaxHandler("https://example.org", os));


        assertThat(new String(os.toByteArray(), StandardCharsets.UTF_8), Is.is("Ramírez-Chaves H E, Mejía Fontecha I Y, Velasquez D, Castaño D (2022): Colección de Mamíferos (Mammalia) del Museo de Historia Natural de la Universidad de Caldas, Colombia. v2.7. Universidad de Caldas. Dataset/Occurrence. https://doi.org/10.15472/mnevig. Accessed at <https://example.org> .\n"));

    }

    @Test
    public void empty() throws ParserConfigurationException, SAXException, IOException {

        InputStream is = getClass().getResourceAsStream("/bio/guoda/preston/empty.xml");

        SAXParser p = SAX_FACTORY.newSAXParser();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        p.parse(is, new CitationSaxHandler("https://example.org", os));


        assertThat(new String(os.toByteArray(), StandardCharsets.UTF_8), Is.is("Accessed at <https://example.org> .\n"));

    }

    @Test
    public void martha() throws ParserConfigurationException, SAXException, IOException {

        InputStream is = getClass().getResourceAsStream("/bio/guoda/preston/martha.eml.xml");

        SAXParser p = SAX_FACTORY.newSAXParser();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        p.parse(is, new CitationSaxHandler("https://example.org", os));


        assertThat(new String(os.toByteArray(), StandardCharsets.UTF_8), Is.is("Martha's Vineyard species checklist. Accessed at <https://example.org> .\n"));

    }

}