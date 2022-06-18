package bio.guoda.preston.cmd;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.InputStream;

public interface TreatmentParser {

    JsonNode parse(InputStream is) throws IOException, TreatmentParseException;
}
