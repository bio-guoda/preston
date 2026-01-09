package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.ZoteroUtil;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.rdf.api.IRI;
import org.globalbioticinteractions.doi.DOI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZoteroFileStreamHandlerRIS extends ZoteroFileStreamHandlerAbstract {

    private final Logger LOG = LoggerFactory.getLogger(ZoteroFileStreamHandlerRIS.class);
    private final Dereferencer<InputStream> timedDereferencer;
    private final Persisting persisting;

    public ZoteroFileStreamHandlerRIS(ContentStreamHandler contentStreamHandler,
                                      OutputStream os,
                                      Persisting persisting,
                                      Dereferencer<InputStream> deref,
                                      IRI provenanceAnchor) {
        super(contentStreamHandler, os, provenanceAnchor);
        this.persisting = persisting;
        this.timedDereferencer = uri -> {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            try {
                return deref.get(uri);
            } finally {
                stopWatch.stop();
            }
        };

    }


    @Override
    void handleZoteroRecord(JsonNode zoteroRecord, String iriString, AtomicBoolean foundAtLeastOne) throws ContentStreamException, IOException {
        ObjectNode zenodoRecord = new ObjectMapper().createObjectNode();

        String zoteroAttachmentDownloadUrl = ZoteroUtil.getAttachmentDownloadUrl(zoteroRecord);
        String providedContentId;

        if (hasPdfAttachment(zoteroRecord, zoteroAttachmentDownloadUrl)) {
            List<String> links = new ArrayList<>();
            String filename = zoteroRecord.at("/data/filename").asText();
            if (StringUtils.isNoneBlank(filename)) {
                ZenodoMetaUtil.setFilename(zenodoRecord, filename);
            }
            String md5 = zoteroRecord.at("/data/md5").asText();
            if (StringUtils.isNoneBlank(md5)) {
                providedContentId = HashType.md5.getPrefix() + md5;
                addZenodoQueryLink(links, providedContentId);

                StreamHandlerUtil.makeActionable(providedContentId);
            }
            String zoteroItemUrl = zoteroRecord.at("/links/up/href").asText();
            IRI zoteroItemIRI = RefNodeFactory.toIRI(zoteroItemUrl);

            DerferencerFactory derferencerFactory = () -> timedDereferencer;


            String zoteroLSID = getZoteroLSID(zoteroItemUrl);

            addZenodoQueryLink(links, zoteroLSID);
            links.add(zoteroLSID);
            links.add(getZoteroSelector(zoteroItemUrl));
            links.add(getZoteroHtmlPage(zoteroItemUrl));

            InputStream itemInputStream = ContentQueryUtil.getContent(
                    zoteroItemIRI,
                    derferencerFactory,
                    LOG
            );

            JsonNode itemData = new ObjectMapper().readTree(itemInputStream);
            if (isPrimaryAttachmentOf(zoteroAttachmentDownloadUrl, itemData)) {
                boolean isLikelyZoteroRecord1 = isIsLikelyZoteroRecord(itemData);
                if (isLikelyZoteroRecord1) {
                    List<String> creatorList = ZoteroUtil.parseCreators(getCreators(itemData));
                    setArray(zenodoRecord, creatorList, RISUtil.RIS_AUTHOR_NAME);

                    List<String> tagValues = ZoteroUtil.parseKeywords(itemData);
                    setArray(zenodoRecord, tagValues, RISUtil.RIS_KEYWORD);

                    String itemType = itemData.at("/data/itemType").asText();
                    setTagValue(zenodoRecord, RISUtil.RIS_PUBLICATION_TYPE, ZoteroUtil.ZOTERO_TO_RIS_PUB_TYPE_TRANSLATION_TABLE.getOrDefault(itemType, "GEN"));
                    String dateStringParsed = ZoteroUtil.getPublicationDate(itemData);
                    setTagValue(zenodoRecord, RISUtil.RIS_PUBLICATION_DATE, dateStringParsed);
                    setTagValue(zenodoRecord, RISUtil.RIS_TITLE, ZoteroUtil.getTitle(itemData));


                    if (StringUtils.equals(itemType, ZoteroUtil.ZOTERO_JOURNAL_ARTICLE)) {
                        setTagValue(zenodoRecord, RISUtil.RIS_JOURNAL_TITLE, ZoteroUtil.getJournalTitle(itemData));
                        setTagValue(zenodoRecord, RISUtil.RIS_JOURNAL_VOLUME, ZoteroUtil.getJournalVolume(itemData));
                        setTagValue(zenodoRecord, RISUtil.RIS_JOURNAL_ISSUE, ZoteroUtil.getJournalIssue(itemData));
                        setTagValue(zenodoRecord, RISUtil.RIS_JOURNAL_PAGES, ZoteroUtil.getJournalPages(itemData));
                    }

                    if (Arrays.asList(ZoteroUtil.ZOTERO_BOOK, ZoteroUtil.ZOTERO_BOOK_SECTION).contains(itemType)) {
                        setTagValue(zenodoRecord, RISUtil.RIS_IMPRINT_PUBLISHER, ZoteroUtil.getPublisherName(itemData));
                        setTagValue(zenodoRecord, RISUtil.RIS_TITLE, ZoteroUtil.getBookTitle(itemData));
                        setTagValue(zenodoRecord, RISUtil.RIS_JOURNAL_PAGES, ZoteroUtil.getJournalVolume(itemData));
                    }

                    DOI doi = ZoteroUtil.getValidDOIOrNull(iriString, itemData, ZoteroUtil.getReference(itemData));
                    if (doi != null) {
                        setTagValue(zenodoRecord, RISUtil.RIS_DOI, doi.toString());
                    }

                    String abstractNote = ZoteroUtil.getAbstract(itemData);
                    if (StringUtils.isNotBlank(abstractNote)) {
                        setTagValue(zenodoRecord, RISUtil.RIS_ABSTRACT, abstractNote);
                    }
                }

                links.add(StreamHandlerUtil.makeActionable(iriString));
                links.add(StreamHandlerUtil.makeActionable(getProvenanceAnchor().getIRIString()));

                setArray(zenodoRecord,
                        links,
                        RISUtil.RIS_URL
                );
                ZenodoMetaUtil.appendIdentifier(zenodoRecord, ZenodoMetaUtil.IS_DERIVED_FROM, StreamHandlerUtil.makeActionable(iriString));
                ZenodoMetaUtil.appendIdentifier(zenodoRecord, ZenodoMetaUtil.IS_PART_OF, getProvenanceAnchor().getIRIString());
                ZenodoMetaUtil.setValue(zenodoRecord, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_PUBLICATION);
                ZenodoMetaUtil.setType(zenodoRecord, "application/json");
                ZenodoMetaUtil.setValue(zenodoRecord, ZenodoMetaUtil.REFERENCE_ID, ZoteroUtil.getReference(itemData).asText());

                if (isLikelyZoteroRecord1) {
                    foundAtLeastOne.set(true);
                    writeRecord(foundAtLeastOne, zenodoRecord);
                }
            }

        }
    }

    private static void addZenodoQueryLink(List<String> links, String query) {
        URI uri;
        try {
            uri = new URI("https", null, "zenodo.org", -1, "/search", "q=\"" + query + "\"", null);
            links.add(uri.toString());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
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

    private static boolean isIsLikelyZoteroRecord(JsonNode jsonNode) {
        boolean isLikelyZoteroRecord = !ZoteroUtil.getReference(jsonNode).isMissingNode()
                && !getCreators(jsonNode).isMissingNode()
                && StringUtils.contains(ZoteroUtil.getReference(jsonNode).asText(), "zotero.org");
        return isLikelyZoteroRecord;
    }

    private static JsonNode getCreators(JsonNode jsonNode) {
        return jsonNode.at("/data/creators");
    }

    private static void setTagValue(ObjectNode objectNode, String tagName, String tagValue) {
        if (StringUtils.isNotBlank(tagValue)) {
            objectNode.put(tagName, tagValue);
        }
    }

    private static void setArray(ObjectNode objectNode, List<String> tagValues, String risKeyword) {
        ArrayNode keywords = new ObjectMapper().createArrayNode();
        tagValues.forEach(keywords::add);
        objectNode.set(risKeyword, keywords);
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
        if (foundAtLeastOne.get()) {
            RISUtil.writeAsRIS(objectNode, getOutputStream());
        }
    }

}
