package bio.guoda.preston.util;

import bio.guoda.preston.RefNodeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class JekyllPageWriterGBIF implements JekyllPageWriter {

    @Override
    public void writePages(InputStream is, JekyllUtil.JekyllPageFactory factory, JekyllUtil.RecordType pageType) throws IOException {
        writePagesImpl(is, factory, pageType);
    }

    private static void writePagesImpl(InputStream is, JekyllUtil.JekyllPageFactory factory, JekyllUtil.RecordType pageType) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode jsonNode = objectMapper.readTree(is);
        writePage(factory, pageType, objectMapper, jsonNode);
    }

    private static void writePage(JekyllUtil.JekyllPageFactory factory,
                                  JekyllUtil.RecordType pageType,
                                  ObjectMapper objectMapper,
                                  JsonNode item) throws IOException {
        if (item.has("key")) {
            final String occurrenceKey = item.get("key").asText();
            try (final OutputStream os = factory.outputStreamFor(RefNodeFactory.toIRI("https://api.gbif.org/v1/occurrence/" + occurrenceKey))) {
                final ObjectNode frontMatter = objectMapper.createObjectNode();
                frontMatter.put("layout", pageType.name());
                frontMatter.put("id", occurrenceKey);
                frontMatter.put("permalink", "/" + occurrenceKey);
                frontMatter.set("gbif", item);

                JekyllUtil.writeFrontMatter(os, frontMatter);
            }
        }
    }
}
