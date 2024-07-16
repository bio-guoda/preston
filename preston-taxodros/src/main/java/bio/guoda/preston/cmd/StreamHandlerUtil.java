package bio.guoda.preston.cmd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamHandlerUtil {

    public static void writeRecord(AtomicBoolean foundAtLeastOne, ObjectNode objectNode, OutputStream outputStream) throws IOException {
        ObjectNode metadata = new ObjectMapper().createObjectNode();
        metadata.set("metadata", objectNode);
        IOUtils.copy(IOUtils.toInputStream(metadata.toString(), StandardCharsets.UTF_8), outputStream);
        IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
        objectNode.removeAll();
        foundAtLeastOne.set(true);
    }

    public static String makeActionable(String providedContentId) {
        return "https://linker.bio/" + providedContentId;
    }
}
