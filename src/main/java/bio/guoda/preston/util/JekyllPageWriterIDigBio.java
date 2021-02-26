package bio.guoda.preston.util;

import bio.guoda.preston.model.RefNodeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

public class JekyllPageWriterIDigBio implements JekyllPageWriter {


    @Override
    public void writePages(InputStream is, JekyllUtil.JekyllPageFactory factory, JekyllUtil.RecordType pageType) throws IOException {
        writePagesImpl(is, factory, pageType);
    }

    private static void writePagesImpl(InputStream is, JekyllUtil.JekyllPageFactory factory, JekyllUtil.RecordType pageType) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode jsonNode = objectMapper.readTree(is);
        writePage(factory, pageType, objectMapper, jsonNode);
        if (jsonNode.has("items")) {
            for (JsonNode item : jsonNode.get("items")) {
                writePage(factory, pageType, objectMapper, item);
            }
        }
    }

    private static void writePage(JekyllUtil.JekyllPageFactory factory,
                                  JekyllUtil.RecordType pageType,
                                  ObjectMapper objectMapper,
                                  JsonNode item) throws IOException {
        if (item.has("uuid")) {
            final String uuid = item.get("uuid").asText();
            try (final OutputStream os = factory.outputStreamFor(RefNodeFactory.toIRI(UUID.fromString(uuid)))) {
                final ObjectNode frontMatter = objectMapper.createObjectNode();
                frontMatter.put("layout", pageType.name());
                frontMatter.put("id", uuid);
                frontMatter.put("permalink", "/" + uuid);
                frontMatter.set("idigbio", item);

                JekyllUtil.writeFrontMatter(os, frontMatter);
            }
        }
    }



}
