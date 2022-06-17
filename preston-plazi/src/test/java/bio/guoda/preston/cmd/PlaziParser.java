package bio.guoda.preston.cmd;

import com.fasterxml.jackson.databind.JsonNode;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;

import static bio.guoda.preston.cmd.PlaziUtil.parseTreatment;

public class PlaziParser implements TreatmentParser {

    @Override
    public JsonNode parse(InputStream is) throws IOException {
        try {
            return parseTreatment(is);
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
            throw new IOException("failed to parse treatment", e);
        }
    }
}
