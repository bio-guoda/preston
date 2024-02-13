package bio.guoda.preston.zenodo;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.zenodo.ZenodoUtils.getObjectMapper;

public class ZenodoMetadataFileStreamHandler implements ContentStreamHandler {


    private final Dereferencer<InputStream> dereferencer;
    private final ZenodoConfig ctx;
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;

    public ZenodoMetadataFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                           Dereferencer<InputStream> inputStreamDereferencer,
                                           OutputStream os,
                                           ZenodoConfig ctx) {
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
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));

            for (int lineNumber = 1; contentStreamHandler.shouldKeepProcessing(); ++lineNumber) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                } else {
                    attemptToHandleJSON(line);
                }
            }
        } catch (IOException e) {
            throw new ContentStreamException("no charset detected", e);
        }

        return foundAtLeastOne.get();
    }

    private void attemptToHandleJSON(String line) throws IOException, ContentStreamException {
        JsonNode zenodoMetadata = getObjectMapper().readTree(line);
        if (maybeContainsPrestonEnabledZenodoMetadata(zenodoMetadata)) {
            List<String> lsids = new ArrayList<>();
            List<String> contentIds = new ArrayList<>();
            JsonNode alternateIdentifiers = zenodoMetadata.at("/metadata/related_identifiers");
            if (alternateIdentifiers != null && alternateIdentifiers.isArray()) {
                for (JsonNode alternateIdentifier : alternateIdentifiers) {
                    JsonNode relation = alternateIdentifier.at("/relation");
                    JsonNode identifier = alternateIdentifier.at("/identifier");
                    if (relation != null && identifier != null) {
                        if (StringUtils.equals(relation.asText(), "isAlternateIdentifier")) {
                            String identiferText = identifier.asText();
                            if (StringUtils.startsWith(identiferText, "urn:lsid")) {
                                lsids.add(identiferText);
                            } else if (StringUtils.startsWith(identiferText, "hash:")){
                                contentIds.add(identiferText);
                            }
                        }
                    }
                }
            }
            if (!lsids.isEmpty()) {
                Collection<Pair<Long, String>> foundDeposits = ZenodoUtils.findByAlternateIds(ctx, lsids);
                List<Long> existingIds = foundDeposits
                        .stream()
                        .filter(x -> !StringUtils.equals(x.getValue(), "unsubmitted"))
                        .map(Pair::getKey)
                        .distinct()
                        .collect(Collectors.toList());

                ZenodoContext ctxLocal = new ZenodoContext(this.ctx);
                if (existingIds.size() == 0) {
                    ZenodoContext newDeposit = ZenodoUtils.create(ctxLocal, zenodoMetadata);
                    uploadContentAndPublish(zenodoMetadata, contentIds, newDeposit);
                } else if (existingIds.size() == 1) {
                    ctxLocal.setDepositId(existingIds.get(0));
                    ZenodoContext newDepositVersion = ZenodoUtils.createNewVersion(ctxLocal);
                    String input = getObjectMapper().writer().writeValueAsString(zenodoMetadata);
                    ZenodoUtils.update(newDepositVersion, input);
                    uploadContentAndPublish(zenodoMetadata, contentIds, newDepositVersion);
                } else {
                    throw new ContentStreamException("found more than one deposit ids (e.g., " + StringUtils.join(existingIds, ", ") + " matching (" + StringUtils.join(lsids, ", ") + ") ");
                }
            }

        }
    }

    private void uploadContentAndPublish(JsonNode taxodrosMetadata, List<String> ids, ZenodoContext ctx) throws IOException {
        uploadAttempt(taxodrosMetadata, ids, ctx);
        ZenodoUtils.publish(ctx);
    }

    private void uploadAttempt(JsonNode taxodrosMetadata, List<String> ids, ZenodoContext ctx) throws IOException {
        JsonNode filename = taxodrosMetadata.at("/metadata/filename");
        if (filename != null) {
            Optional<IRI> contentIdCandidate = ids.stream()
                    .map(RefNodeFactory::toIRI)
                    .filter(HashKeyUtil::isValidHashKey)
                    .findFirst();
            if (contentIdCandidate.isPresent()) {
                ZenodoUtils.upload(ctx,
                        filename.asText(),
                        new DerferencingEntity(dereferencer, contentIdCandidate.get()));
            }
        }
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
