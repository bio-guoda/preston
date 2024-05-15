package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.cmd.JsonObjectDelineator;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.LongStream;

import static bio.guoda.preston.RefNodeConstants.ALTERNATE_OF;
import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.toBlank;

public class RegistryReaderZotero extends ProcessorReadOnly {
    public static final Pattern URL_PATTERN_ZOTERO_GROUP_HTML = Pattern.compile("http[s]{0,1}://(www\\.){0,1}(zotero\\.org/groups/)(?<groupIdOrName>[^/]+)(/.*){0,1}");
    public static final Pattern URL_PATTERN_ZOTERO_GROUP_API = Pattern.compile("https://api\\.zotero\\.org/groups/(?<groupId>[0-9]+)");
    public static final Pattern URL_PATTERN_ZOTERO_GROUP_ITEMS_API = Pattern.compile("https://api\\.zotero\\.org/groups/[^/]+/items[^/]*");
    private static final Logger LOG = LoggerFactory.getLogger(RegistryReaderZotero.class);

    public RegistryReaderZotero(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (RefNodeFactory.hasVersionAvailable(statement)) {
            String sourceIri = getVersionSource(statement).getIRIString();
            Matcher matcher = URL_PATTERN_ZOTERO_GROUP_HTML.matcher(sourceIri);
            if (matcher.matches()) {
                requestGroupMetadata(sourceIri, this);
            }
            Matcher matcher1 = URL_PATTERN_ZOTERO_GROUP_API.matcher(sourceIri);
            if (matcher1.matches()) {
                try {
                    emitRequestForItems(get((IRI) getVersion(statement)), this);
                } catch (IOException e) {
                    LOG.warn("failed to handle [" + statement + "]", e);
                }
            }

            Matcher matcher2 = URL_PATTERN_ZOTERO_GROUP_ITEMS_API.matcher(sourceIri);
            if (matcher2.matches() && !StringUtils.endsWith(sourceIri, "file/view")) {
                try {
                    requestItemAttachments(get((IRI) getVersion(statement)), this);
                    requestJsonObjectMembers((IRI) getVersion(statement), this);
                } catch (IOException e) {
                    LOG.warn("failed to handle [" + statement + "]", e);
                }
            }
        }
    }

    private void requestJsonObjectMembers(IRI itemsPage, StatementsEmitter emitter) throws IOException {

        JsonObjectDelineator.locateTopLevelObjects(get(itemsPage), new Consumer<Pair<Long, Long>>() {
            @Override
            public void accept(Pair<Long, Long> objectStartFinishRange) {
                IRI jsonObjectContentId
                        = RefNodeFactory.toIRI("cut:" + itemsPage.getIRIString() + "!/b" + objectStartFinishRange.getLeft() + "-" + objectStartFinishRange.getRight());
                emitter.emit(Arrays.asList(
                        RefNodeFactory.toStatement(
                                jsonObjectContentId,
                                HAS_FORMAT,
                                RefNodeFactory.toLiteral("application/json+zotero")
                        ),
                        RefNodeFactory.toStatement(
                                itemsPage,
                                HAD_MEMBER,
                                jsonObjectContentId
                        ),
                        RefNodeFactory.toStatement(
                                jsonObjectContentId,
                                HAS_VERSION,
                                jsonObjectContentId
                        )
                ));
            }
        });

    }

    public static void requestItemAttachments(InputStream is, StatementEmitter emitter) throws IOException {
        for (JsonNode item : new ObjectMapper().readTree(is)) {
            JsonNode itemUrl = item.at("/links/self/href");
            if (itemUrl != null) {
                String attachmentDownloadUrlString = ZoteroUtil.getZoteroAttachmentDownloadUrl(item);

                if (StringUtils.isNoneBlank(attachmentDownloadUrlString)) {
                    JsonNode attachmentType = item.at("/links/attachment/attachmentType");
                    if (attachmentType != null && StringUtils.isNotBlank(attachmentType.asText())) {
                        emitter.emit(RefNodeFactory.toStatement(
                                RefNodeFactory.toIRI(attachmentDownloadUrlString),
                                HAS_FORMAT,
                                RefNodeFactory.toLiteral("application/pdf"))
                        );
                    }

                    emitter.emit(RefNodeFactory.toStatement(
                            RefNodeFactory.toIRI(attachmentDownloadUrlString),
                            HAS_VERSION,
                            RefNodeFactory.toBlank())
                    );


                }
            }
        }
    }


    public static void requestGroupMetadata(String url, StatementEmitter statementEmitter) {
        String groupIdOrName = getGroupIdOrName(url);
        if (StringUtils.isNotBlank(groupIdOrName)) {
            String zoteroGroupRequest = "https://api.zotero.org/groups/" + groupIdOrName;
            statementEmitter.emit(RefNodeFactory.toStatement(
                    RefNodeFactory.toIRI(zoteroGroupRequest),
                    ALTERNATE_OF,
                    RefNodeFactory.toIRI(url)
            ));
            statementEmitter.emit(RefNodeFactory.toStatement(
                    RefNodeFactory.toIRI(zoteroGroupRequest),
                    HAS_VERSION,
                    RefNodeFactory.toBlank()
            ));

        }
    }

    public static String getGroupIdOrName(String urlStringCandidate) {
        Matcher matcher = URL_PATTERN_ZOTERO_GROUP_HTML
                .matcher(urlStringCandidate);
        return matcher.matches() ? matcher.group("groupIdOrName") : null;
    }

    public static void emitIRequestsForItems(long numberOfItems, long groupId, StatementEmitter emitter) {
        int pageSize = 100;
        LongStream.rangeClosed(0, numberOfItems / pageSize)
                .mapToObj(page -> "https://api.zotero.org/groups/" + groupId + "/items?start=" + page * pageSize + "&limit=100")
                .map(url ->
                        RefNodeFactory.toStatement(
                                RefNodeFactory.toIRI(url),
                                HAS_VERSION,
                                RefNodeFactory.toBlank()))
                .forEach(emitter::emit);
    }

    public static void emitRequestForItems(InputStream is, StatementEmitter emitter) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(is);
        JsonNode groupIdString = jsonNode.at("/id");
        JsonNode numItems = jsonNode.at("/meta/numItems");
        long numberOfItems = numItems.longValue();
        long groupId = groupIdString.longValue();
        if (numberOfItems > 0 && groupId > 0) {
            emitIRequestsForItems(numberOfItems, groupId, emitter);
        }
    }

}
