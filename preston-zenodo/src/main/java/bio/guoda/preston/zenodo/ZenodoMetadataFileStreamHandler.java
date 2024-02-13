package bio.guoda.preston.zenodo;

import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ZenodoMetadataFileStreamHandler implements ContentStreamHandler {


    private final Dereferencer<InputStream> dereferencer;
    private final ZenodoContext ctx;
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;

    public ZenodoMetadataFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                           Dereferencer<InputStream> inputStreamDereferencer,
                                           OutputStream os,
                                           ZenodoContext ctx) {
        this.contentStreamHandler = contentStreamHandler;
        this.dereferencer = inputStreamDereferencer;
        this.outputStream = os;
        this.ctx = ctx;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        String iriString = version.getIRIString();
        try {
            Charset charset = new UniversalEncodingDetector().detect(is, new Metadata());
            if (charset != null) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, charset));

                for (int lineNumber = 1; contentStreamHandler.shouldKeepProcessing(); ++lineNumber) {
                    String line = reader.readLine();
                    if (line == null) {
                        break;
                    } else {
                        JsonNode jsonNode = ZenodoUtils.getObjectMapper().readTree(line);
                        System.out.println(jsonNode.toPrettyString());
                        if (maybeContainsPrestonEnabledZenodoMetadata(jsonNode)) {
                            List<String> ids = new ArrayList<>();
                            JsonNode alternateIdentifiers = jsonNode.at("/metadata/related_identifiers");
                            if (alternateIdentifiers != null && alternateIdentifiers.isArray()) {
                                for (JsonNode alternateIdentifier : alternateIdentifiers) {
                                    JsonNode relation = alternateIdentifier.at("/relation");
                                    JsonNode identifier = alternateIdentifier.at("/identifier");
                                    if (relation != null && identifier != null) {
                                        if (StringUtils.equals(relation.asText(), "isAlternateIdentifier")) {
                                            ids.add(identifier.asText());
                                        }
                                    }
                                }
                            }
                            if (!ids.isEmpty()) {
                                Collection<Pair<Long, String>> foundDeposits = ZenodoUtils.findByAlternateIds(ctx, ids);
                                Stream<Long> publishedMatches = foundDeposits
                                        .stream()
                                        .filter(x -> !StringUtils.equals(x.getValue(), "unsubmitted"))
                                        .map(Pair::getKey);

                                List<Long> collect = publishedMatches.collect(Collectors.toList());
                                if (collect.size() == 0) {
                                    ZenodoUtils.create(ctx, jsonNode);
                                } else if (collect.size() == 1) {
                                    // create new version and update
                                } else  {
                                    throw new ContentStreamException("found more than one deposit ids (e.g., " + StringUtils.join(collect, ", ") + " matching (" + StringUtils.join(ids, ", ") + ") ");
                                }
                            }

                        }
                    }
                }
            }

        } catch (IOException e) {
            throw new ContentStreamException("no charset detected");
        }

        return foundAtLeastOne.get();
    }

    private boolean maybeContainsPrestonEnabledZenodoMetadata(JsonNode jsonNode) {
        return jsonNode.at("/metadata/upload_type") != null
                && jsonNode.at("/metadata/title") != null
                && jsonNode.at("/metadata/related_identifiers") != null;
    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
