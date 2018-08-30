package org.globalbioticinteractions.preston.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.MimeTypes;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefStatement;
import org.globalbioticinteractions.preston.model.RefNodeString;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.globalbioticinteractions.preston.RefNodeConstants.CONTINUED_AT;
import static org.globalbioticinteractions.preston.RefNodeConstants.DATASET_REGISTRY_OF;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_CONTENT;

public class RegistryReaderGBIF extends RefStatementProcessor {
    private static final Map<String, String> SUPPORTED_ENDPOINT_TYPES = new HashMap<String, String>() {{
        put("DWC_ARCHIVE", MimeTypes.MIME_TYPE_DWCA);
        put("EML", MimeTypes.MIME_TYPE_EML);
    }};

    private static final String GBIF_DATASET_API_ENDPOINT = "https://api.gbif.org/v1/dataset";
    private final Log LOG = LogFactory.getLog(RegistryReaderGBIF.class);

    public RegistryReaderGBIF(RefStatementListener listener) {
        super(listener);
    }

    @Override
    public void on(RefStatement statement) {
        if (statement.getTarget().equivalentTo(Seeds.SEED_NODE_GBIF)) {
            RefNode refNodeRegistry = new RefNodeString(GBIF_DATASET_API_ENDPOINT);
            emit(new RefStatement(statement.getTarget(), DATASET_REGISTRY_OF, refNodeRegistry));
            emit(new RefStatement(refNodeRegistry, HAS_CONTENT, null));

        } else if (statement.getTarget() != null
                && statement.getSource().getLabel().startsWith(GBIF_DATASET_API_ENDPOINT)
                && statement.getRelationType().equals(RefNodeConstants.HAS_CONTENT)) {
            try {
                parse(statement.getTarget().getContent(), this, statement.getTarget());
            } catch (IOException e) {
                LOG.warn("failed to handle [" + statement.getLabel() + "]", e);
            }
        } else if (statement.getTarget() != null) {
            if (StringUtils.startsWith(statement.getTarget().getLabel(), GBIF_DATASET_API_ENDPOINT)) {
                emit(new RefStatement(statement.getTarget(), HAS_CONTENT, null));
            }
        }
    }

    private static void emitNextPage(RefNode previousPage, int offset, int limit, RefStatementEmitter emitter) {
        String uri = GBIF_DATASET_API_ENDPOINT + "?offset=" + offset + "&limit=" + limit;
        RefNode nextPage = new RefNodeString(uri);
        emitter.emit(new RefStatement(previousPage, CONTINUED_AT, nextPage));

    }

    public static void parse(InputStream resourceAsStream, RefStatementEmitter emitter, RefNode dataset) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(resourceAsStream);
        if (jsonNode != null && jsonNode.has("results")) {
            for (JsonNode result : jsonNode.get("results")) {
                if (result.has("key") && result.has("endpoints")) {
                    String uuid = result.get("key").asText();
                    RefNodeString datasetUUID = new RefNodeString(uuid);
                    emitter.emit(new RefStatement(dataset, RefNodeConstants.HAS_PART, datasetUUID));

                    for (JsonNode endpoint : result.get("endpoints")) {
                        if (endpoint.has("url") && endpoint.has("type")) {
                            String urlString = endpoint.get("url").asText();
                            String type = endpoint.get("type").asText();

                            if (SUPPORTED_ENDPOINT_TYPES.containsKey(type)) {
                                RefNodeString dataArchive = new RefNodeString(urlString);
                                emitter.emit(new RefStatement(datasetUUID, RefNodeConstants.HAS_PART, dataArchive));
                                emitter.emit(new RefStatement(dataArchive, RefNodeConstants.HAS_FORMAT, new RefNodeString(SUPPORTED_ENDPOINT_TYPES.get(type))));
                                emitter.emit(new RefStatement(dataArchive, RefNodeConstants.HAS_CONTENT, null));
                            }
                        }
                    }
                }
            }
        }

        boolean endOfRecords = jsonNode == null || (!jsonNode.has("endOfRecords") || jsonNode.get("endOfRecords").asBoolean(true));
        if (!endOfRecords && jsonNode.has("offset") && jsonNode.has("limit")) {
            int offset = jsonNode.get("offset").asInt();
            int limit = jsonNode.get("limit").asInt();
            emitNextPage(dataset, offset + limit, limit, emitter);
        }

    }

}
