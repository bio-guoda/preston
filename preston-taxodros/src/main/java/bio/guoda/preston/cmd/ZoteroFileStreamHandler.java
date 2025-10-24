package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.ZoteroUtil;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZoteroFileStreamHandler implements ContentStreamHandler {
    public static final String ZOTERO_JOURNAL_ARTICLE = "journalArticle";
    public static final String ZOTERO_BOOK = "book";
    public static final String ZOTERO_BOOK_SECTION = "bookSection";
    public static final String ZOTERO_PREPRINT = "preprint";
    public static final String ZOTERO_REPORT = "report";
    public static final String ZOTERO_THESIS = "thesis";
    public static final String ZOTERO_CONFERENCE_PAPER = "conferencePaper";
    private final Logger LOG = LoggerFactory.getLogger(ZoteroFileStreamHandler.class);


    private final Persisting persisting;
    private final Dereferencer<InputStream> timedDereferencer;
    private final IRI provenanceAnchor;
    private final ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;
    private final List<String> communities;

    public ZoteroFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                   OutputStream os,
                                   Persisting persisting,
                                   Dereferencer<InputStream> deref,
                                   List<String> communities,
                                   IRI provenanceAnchor) {
        this.contentStreamHandler = contentStreamHandler;
        this.outputStream = os;
        this.persisting = persisting;
        this.timedDereferencer = new Dereferencer<InputStream>() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                try {
                    return deref.get(uri);
                } finally {
                    stopWatch.stop();
                }
            }
        };
        this.communities = communities;
        this.provenanceAnchor = provenanceAnchor;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        String iriString = version.getIRIString();
        try {
            JsonNode zoteroRecord = new ObjectMapper().readTree(is);
            if (zoteroRecord.isObject()) {
                ObjectNode zenodoRecord = new ObjectMapper().createObjectNode();

                String zoteroAttachmentDownloadUrl = ZoteroUtil.getAttachmentDownloadUrl(zoteroRecord);
                String providedContentId = null;

                if (hasPdfAttachment(zoteroRecord, zoteroAttachmentDownloadUrl)) {
                    String filename = zoteroRecord.at("/data/filename").asText();
                    if (StringUtils.isNoneBlank(filename)) {
                        ZenodoMetaUtil.setFilename(zenodoRecord, filename);
                    }
                    String md5 = zoteroRecord.at("/data/md5").asText();
                    if (StringUtils.isNoneBlank(md5)) {
                        providedContentId = HashType.md5.getPrefix() + md5;
                        ZenodoMetaUtil.appendIdentifier(zenodoRecord, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, providedContentId);
                    }
                    String zoteroItemUrl = zoteroRecord.at("/links/up/href").asText();
                    appendAttachmentInfo(
                            zenodoRecord,
                            StringUtils.isBlank(providedContentId) ? zoteroAttachmentDownloadUrl : providedContentId,
                            zoteroItemUrl
                    );

                    IRI zoteroItemIRI = RefNodeFactory.toIRI(zoteroItemUrl);

                    DerferencerFactory derferencerFactory = () -> timedDereferencer;

                    InputStream itemInputStream = ContentQueryUtil.getContent(
                            zoteroItemIRI,
                            derferencerFactory,
                            LOG
                    );

                    JsonNode itemData = new ObjectMapper().readTree(itemInputStream);
                    if (isPrimaryAttachmentOf(zoteroAttachmentDownloadUrl, itemData)) {
                        boolean isLikelyZoteroRecord = appendJournalArticleMetaData(
                                iriString,
                                itemData,
                                zenodoRecord
                        );

                        ZenodoMetaUtil.appendIdentifier(zenodoRecord, ZenodoMetaUtil.IS_COMPILED_BY, RefNodeConstants.PRESTON_DOI, ZenodoMetaUtil.RESOURCE_TYPE_SOFTWARE);

                        if (isLikelyZoteroRecord) {
                            foundAtLeastOne.set(true);
                            writeRecord(foundAtLeastOne, zenodoRecord);
                        }
                    }

                }


            }
        } catch (IOException e) {
            // opportunistic parsing, so ignore exceptions
        } catch (IllegalArgumentException ex) {
            LOG.warn("possible marformed Zotero records in [" + version + "]", ex);
        }
        return foundAtLeastOne.get();
    }

    private boolean isPrimaryAttachmentOf(String zoteroAttachmentDownloadUrl, JsonNode itemData) {
        String primaryAttachment = itemData.at("/links/attachment/href").asText();
        return StringUtils.isNotBlank(primaryAttachment) && StringUtils.startsWith(zoteroAttachmentDownloadUrl, primaryAttachment);
    }

    private boolean hasPdfAttachment(JsonNode zoteroRecord, String zoteroAttachmentDownloadUrl) {
        boolean hasPdfAttachment = false;
        if (StringUtils.isNoneBlank(zoteroAttachmentDownloadUrl)) {
            String contentType = zoteroRecord.at("/data/contentType").asText();
            if (StringUtils.equals(contentType, "application/pdf")) {
                hasPdfAttachment = true;
            }
        }
        return hasPdfAttachment;
    }

    private boolean appendJournalArticleMetaData(String iriString, JsonNode jsonNode, ObjectNode objectNode) {
        JsonNode reference = jsonNode.at("/links/self/href");
        JsonNode creators = jsonNode.at("/data/creators");
        boolean isLikelyZoteroRecord = !reference.isMissingNode()
                && !creators.isMissingNode()
                && StringUtils.contains(reference.asText(), "zotero.org");
        if (isLikelyZoteroRecord) {

            ZenodoMetaUtil.setCommunities(objectNode, communities.stream());

            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.WAS_DERIVED_FROM, StreamHandlerUtil.makeActionable(iriString));
            ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, StreamHandlerUtil.makeActionable(iriString));
            ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_PART_OF, provenanceAnchor.getIRIString());
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_PUBLICATION);
            ZenodoMetaUtil.setType(objectNode, "application/json");
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.REFERENCE_ID, reference.asText());

            List<String> creatorList = parseCreators(creators);
            ZenodoMetaUtil.setCreators(objectNode, creatorList);

            JsonNode tags = jsonNode.at("/data/tags");
            if (!tags.isMissingNode() && tags.isArray()) {
                tags.forEach(t -> {
                    if (t.has("tag")) {
                        ZenodoMetaUtil.addKeyword(objectNode, t.get("tag").asText());
                    }
                });
            }

            String itemType = jsonNode.at("/data/itemType").asText();
            Map<String, String> typeTranslationTable = new TreeMap<String, String>() {{
                put(ZOTERO_JOURNAL_ARTICLE, ZenodoMetaUtil.PUBLICATION_TYPE_ARTICLE);
                put(ZOTERO_BOOK, ZenodoMetaUtil.PUBLICATION_TYPE_BOOK);
                put(ZOTERO_BOOK_SECTION, ZenodoMetaUtil.PUBLICATION_TYPE_BOOK_SECTION);
                put(ZOTERO_PREPRINT, ZenodoMetaUtil.PUBLICATION_TYPE_PREPRINT);
                put(ZOTERO_REPORT, ZenodoMetaUtil.PUBLICATION_TYPE_REPORT);
                put(ZOTERO_THESIS, ZenodoMetaUtil.PUBLICATION_TYPE_THESIS);
                put(ZOTERO_CONFERENCE_PAPER, ZenodoMetaUtil.PUBLICATION_TYPE_CONFERENCE_PAPER);
            }};

            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.PUBLICATION_TYPE, typeTranslationTable.getOrDefault(itemType, "other"));
            String dateString = jsonNode.at("/data/date").asText();
            String dateStringParsed = parseDate(dateString);
            ZenodoMetaUtil.setPublicationDate(objectNode, dateStringParsed);
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.TITLE, jsonNode.at("/data/title").asText());


            if (StringUtils.equals(itemType, ZOTERO_JOURNAL_ARTICLE)) {
                ZenodoMetaUtil.setValueIfNotBlank(objectNode, ZenodoMetaUtil.JOURNAL_TITLE, jsonNode.at("/data/publicationTitle").asText());
                ZenodoMetaUtil.setValueIfNotBlank(objectNode, ZenodoMetaUtil.JOURNAL_VOLUME, jsonNode.at("/data/volume").asText());
                ZenodoMetaUtil.setValueIfNotBlank(objectNode, ZenodoMetaUtil.JOURNAL_ISSUE, jsonNode.at("/data/issue").asText());
                ZenodoMetaUtil.setValueIfNotBlank(objectNode, ZenodoMetaUtil.JOURNAL_PAGES, jsonNode.at("/data/pages").asText());
            }

            if (Arrays.asList(ZOTERO_BOOK, ZOTERO_BOOK_SECTION).contains(itemType)) {
                ZenodoMetaUtil.setValueIfNotBlank(objectNode, ZenodoMetaUtil.IMPRINT_PUBLISHER, jsonNode.at("/data/publisher").asText());
                ZenodoMetaUtil.setValueIfNotBlank(objectNode, ZenodoMetaUtil.PARTOF_PAGES, jsonNode.at("/data/pages").asText());
                ZenodoMetaUtil.setValueIfNotBlank(objectNode, ZenodoMetaUtil.PARTOF_TITLE, jsonNode.at("/data/bookTitle").asText());
            }

            ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, jsonNode.at("/data/DOI").asText());


            StringBuilder description = new StringBuilder();

            if (communities.stream().anyMatch(name -> StringUtils.contains(name, "batlit"))) {
                ZenodoMetaUtil.addKeyword(objectNode, "Biodiversity");
                ZenodoMetaUtil.addKeyword(objectNode, "Mammalia");
                ZenodoMetaUtil.addKeyword(objectNode, "Chiroptera");
                ZenodoMetaUtil.addKeyword(objectNode, "Chordata");
                ZenodoMetaUtil.addKeyword(objectNode, "Animalia");
                ZenodoMetaUtil.addKeyword(objectNode, "bats");
                ZenodoMetaUtil.addKeyword(objectNode, "bat");

                ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_KINGDOM, "Animalia");
                ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_PHYLUM, "Chordata");
                ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_CLASS, "Mammalia");
                ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_ORDER, "Chiroptera");

                description.append("(Uploaded by Plazi for the Bat Literature Project) ");
            }

            String abstractNote = jsonNode.at("/data/abstractNote").textValue();

            String abstractString = StringUtils.isBlank(abstractNote)
                    ? "No abstract provided."
                    : abstractNote;
            description.append(abstractString);

            String descriptionString = description.toString();
            objectNode.put("description",
                    StringUtils.isBlank(descriptionString) ? "No description." : descriptionString
            );
        }
        return isLikelyZoteroRecord;
    }

    static List<String> parseCreators(JsonNode creators) {
        List<String> creatorList = new ArrayList<>();
        if (creators.isArray()) {
            for (JsonNode creator : creators) {
                if (creator.has("firstName") && creator.has("lastName")) {
                    creatorList.add(creator.get("lastName").asText() + ", " + creator.get("firstName").asText());
                } else if (creator.has("name")) {
                    creatorList.add(creator.get("name").asText());
                }
            }
        }
        return creatorList;
    }

    static String parseDate(String publicationDate) {
        String publicationDate8601 = null;
        Pattern iso8601 = Pattern.compile(".*(?<year>[0-9]{4})(?<month>[-][01][0-9]){0,1}(?<day>[-][0123][0-9]){0,1}.*");
        Matcher matcher = iso8601.matcher(publicationDate);
        if (matcher.matches()) {
            publicationDate8601 =
                    matcher.group("year")
                            + StringUtils.defaultIfBlank(matcher.group("month"), "")
                            + StringUtils.defaultIfBlank(matcher.group("day"), "");
        }
        return publicationDate8601;
    }


    private void appendAttachmentInfo(ObjectNode objectNode, String zoteroAttachmentDownloadUrl, String zoteroItemUrl) throws ContentStreamException {
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, getZoteroLSID(zoteroItemUrl));
        appendContentId(objectNode, zoteroAttachmentDownloadUrl, HashType.md5);
        appendContentId(objectNode, zoteroAttachmentDownloadUrl, HashType.sha256);
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, getZoteroSelector(zoteroItemUrl));
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, getZoteroHtmlPage(zoteroItemUrl));
    }

    private void appendContentId(ObjectNode objectNode, String zoteroAttachmentDownloadUrl, HashType hashType) throws ContentStreamException {

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        try {
            StreamHandlerUtil.appendContentId(
                    objectNode,
                    zoteroAttachmentDownloadUrl,
                    hashType,
                    this.timedDereferencer,
                    this.persisting
            );
        } finally {
            stopWatch.stop();
            ContentQueryUtil.logReponseTime(stopWatch, zoteroAttachmentDownloadUrl, "contentid calculated in ", LOG);
        }

    }

    private String getZoteroSelector(String href) {
        return "zotero://select" + URI.create(href).getPath();
    }

    private String getZoteroHtmlPage(String href) {
        return "https://zotero.org" + URI.create(href).getPath();
    }

    private String getZoteroLSID(String href) {
        return "urn:lsid:zotero.org" + StringUtils.replace(URI.create(href).getPath(), "/", ":");
    }


    private void writeRecord(AtomicBoolean foundAtLeastOne, ObjectNode objectNode) throws IOException {
        OutputStream outputStream = this.outputStream;
        StreamHandlerUtil.writeRecord(foundAtLeastOne, objectNode, outputStream);
    }


    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
