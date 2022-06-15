package bio.guoda.preston.cmd;

import bio.guoda.preston.cmd.PlaziUtil;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class PlaziTreatmentStreamHandler implements ContentStreamHandler {

    public static final String META_XML = "meta.xml";
    private final Dereferencer<InputStream> dereferencer;
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;

    public PlaziTreatmentStreamHandler(ContentStreamHandler contentStreamHandler,
                                       Dereferencer<InputStream> inputStreamDereferencer,
                                       OutputStream os) {
        this.contentStreamHandler = contentStreamHandler;
        this.dereferencer = inputStreamDereferencer;
        this.outputStream = os;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        String iriString = version.getIRIString();
        try {
            handleAssumedPlaziTreatment(is, iriString, outputStream);
            return true;
        } catch (IOException | SAXException e) {
            // opportunistic parsing, skip those with exceptions
            return false;
        }
    }

    protected static void handleAssumedPlaziTreatment(InputStream is,
                                                      String iriString,
                                                      OutputStream outputStream) throws SAXException, IOException, ContentStreamException {
        try {
            ObjectNode treatment = new ObjectMapper().createObjectNode();
            treatment.set("http://www.w3.org/ns/prov#wasDerivedFrom", TextNode.valueOf(iriString));
            treatment.set("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", TextNode.valueOf("application/plazi+xml"));

            PlaziUtil.parseTreatment(is, treatment);
            if (treatment.size() > 2) {
                IOUtils.copy(IOUtils.toInputStream(treatment.toString(), StandardCharsets.UTF_8), outputStream);
                IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
            }

        } catch (ParserConfigurationException | XPathExpressionException e) {
            // ignore
        }


    }


    @Override
    public boolean shouldKeepReading() {
        return contentStreamHandler.shouldKeepReading();
    }


}
