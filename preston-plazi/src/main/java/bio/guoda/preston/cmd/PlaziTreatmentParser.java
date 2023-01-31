package bio.guoda.preston.cmd;

import com.fasterxml.jackson.databind.JsonNode;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;

import static bio.guoda.preston.cmd.PlaziUtil.parseTreatment;

public class PlaziTreatmentParser implements TreatmentParser {

    @Override
    public JsonNode parse(InputStream is) throws IOException, TreatmentParseException {
        try {
            return parseTreatment(is);
        } catch (ParserConfigurationException | SAXException | XPathExpressionException e) {
            throw new TreatmentParseException("failed to parseQuads treatment", e);
        } catch (IOException e) {
            throw new IOException("error receiving treatment", e);
        }
    }
}
