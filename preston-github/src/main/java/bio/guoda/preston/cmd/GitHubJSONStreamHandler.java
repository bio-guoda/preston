package bio.guoda.preston.cmd;

import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

public class GitHubJSONStreamHandler implements ContentStreamHandler {

    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;

    public GitHubJSONStreamHandler(ContentStreamHandler contentStreamHandler,
                                   Dereferencer<InputStream> inputStreamDereferencer,
                                   OutputStream os) {
        this.contentStreamHandler = contentStreamHandler;
        this.outputStream = os;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        try {
            Charset charset = new UniversalEncodingDetector().detect(is, new Metadata());
            if (charset != null) {
                try {
                    JsonNode jsonNode = new ObjectMapper().readTree(is);

                    JsonNode candidateNode = jsonNode.isArray() && jsonNode.size() > 0
                            ? jsonNode.get(0)
                            : jsonNode;

                    if (hasGithubAPIURL(candidateNode)) {
                        if (jsonNode.isArray()) {
                            for (JsonNode node : jsonNode) {
                                writeAsLineJSON(version, node);
                            }
                        } else {
                            writeAsLineJSON(version, jsonNode);
                        }
                        foundAtLeastOne.set(true);
                    }
                } catch (JsonProcessingException ex) {
                    // ignore assumed malformed json
                }
            }
        } catch (IOException e) {
            throw new ContentStreamException("cannot handle non-github metadata JSON", e);
        }
        return foundAtLeastOne.get();
    }

    private void writeAsLineJSON(IRI version, JsonNode jsonNode) throws IOException {
        ((ObjectNode) jsonNode).set("http://www.w3.org/ns/prov#wasDerivedFrom", TextNode.valueOf(version.getIRIString()));
        ((ObjectNode) jsonNode).set("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", TextNode.valueOf(ResourcesHTTP.MIMETYPE_GITHUB_JSON));
        IOUtils.copy(IOUtils.toInputStream(jsonNode.toString(), StandardCharsets.UTF_8), outputStream);
        IOUtils.write("\n", outputStream, StandardCharsets.UTF_8);
    }

    private boolean hasGithubAPIURL(JsonNode obj) {
        return obj.has("url")
                && StringUtils.startsWith(obj.get("url").asText(), "https://api.github.com/repos/");
    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
