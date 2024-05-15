package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.ZoteroUtil;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class ZoteroFileStreamHandler implements ContentStreamHandler {
    private final Logger LOG = LoggerFactory.getLogger(ZoteroFileStreamHandler.class);


    private final Persisting persisting;
    private final Dereferencer<InputStream> dereferencer;
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;

    public ZoteroFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                   OutputStream os,
                                   Persisting persisting,
                                   Dereferencer<InputStream> dereferencer) {
        this.contentStreamHandler = contentStreamHandler;
        this.outputStream = os;
        this.persisting = persisting;
        this.dereferencer = dereferencer;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        String iriString = version.getIRIString();
        try {
            JsonNode candidateContent = new ObjectMapper().readTree(is);
            if (candidateContent.isObject()) {
                handleZoteroItem(foundAtLeastOne, iriString, candidateContent);
            }
        } catch (IOException e) {
            // opportunistic parsing, so ignore exceptions
        }
        return foundAtLeastOne.get();
    }

    private void handleZoteroItem(AtomicBoolean foundAtLeastOne, String iriString, JsonNode jsonNode) throws ContentStreamException, IOException {
        boolean likelyZoteroRecord;
        JsonNode reference = jsonNode.at("/links/self/href");
        JsonNode creators = jsonNode.at("/data/creators");
        likelyZoteroRecord = !reference.isMissingNode()
                && !creators.isMissingNode()
                && StringUtils.contains(reference.asText(), "zotero.org");
        if (likelyZoteroRecord) {
            ObjectNode objectNode = new ObjectMapper().createObjectNode();

            ZenodoMetaUtil.setCommunities(objectNode, Stream.of("batlit", "biosyslit"));

            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.WAS_DERIVED_FROM, iriString);
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.UPLOAD_TYPE, ZenodoMetaUtil.UPLOAD_TYPE_PUBLICATION);
            ZenodoMetaUtil.setType(objectNode, "application/json+zotero");
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.REFERENCE_ID, reference.asText());
            ZenodoMetaUtil.setValue(objectNode, "filename", "doc.pdf");

            List<String> creatorList = new ArrayList<>();
            if (creators.isArray()) {
                for (JsonNode creator : creators) {
                    if (creator.has("firstName") && creator.has("lastName")) {
                        creatorList.add(creator.get("lastName").asText() + ", " + creator.get("firstName").asText());
                    }
                }
            }
            ZenodoMetaUtil.setCreators(objectNode, creatorList);

            JsonNode tags = jsonNode.at("/data/tags");
            if (!tags.isMissingNode() && tags.isArray()) {
                tags.forEach(t -> {
                    if (t.has("tag")) {
                        ZenodoMetaUtil.addKeyword(objectNode, t.get("tag").asText());
                    }
                });
            }

            if (StringUtils.equals(jsonNode.at("/data/itemType").asText(), "journalArticle")) {
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.PUBLICATION_TYPE, ZenodoMetaUtil.PUBLICATION_TYPE_ARTICLE);
                String dateString = jsonNode.at("/meta/parsedDate").asText();
                if (!StringUtils.isBlank(dateString)) {
                    ZenodoMetaUtil.setPublicationDate(objectNode, dateString);
                }
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_TITLE, jsonNode.at("/data/publicationTitle").asText());
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.TITLE, jsonNode.at("/data/title").asText());
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_VOLUME, jsonNode.at("/data/volume").asText());
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_ISSUE, jsonNode.at("/data/issue").asText());
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_PAGES, jsonNode.at("/data/pages").asText());
            }

            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.DOI, jsonNode.at("/data/DOI").asText());

            String attachementUrl = jsonNode.at("/links/attachment/href").asText();
            if (StringUtils.isNoneBlank(attachementUrl)) {
                String zoteroAttachmentDownloadUrl = ZoteroUtil.getZoteroAttachmentDownloadUrl(jsonNode);
                String zoteroItemUrl = jsonNode.at("/links/self/href").asText();
                ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER, getZoteroLSID(zoteroItemUrl));
                appendContentId(objectNode, zoteroAttachmentDownloadUrl, HashType.md5);
                appendContentId(objectNode, zoteroAttachmentDownloadUrl, HashType.sha256);
                ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, getZoteroSelector(zoteroItemUrl));
                ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, getZoteroHtmlPage(zoteroItemUrl));

            }

            ZenodoMetaUtil.addKeyword(objectNode, "Biodiversity");
            ZenodoMetaUtil.addKeyword(objectNode, "Mammalia");
            ZenodoMetaUtil.addKeyword(objectNode, "Chiroptera");
            ZenodoMetaUtil.addKeyword(objectNode, "Chordata");
            ZenodoMetaUtil.addKeyword(objectNode, "Animalia");

            ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_KINGDOM, "Animalia");
            ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_PHYLUM, "Chordata");
            ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_CLASS, "Mammalia");
            ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_DWC_ORDER, "Chiroptera");

            foundAtLeastOne.set(true);
            writeRecord(foundAtLeastOne, objectNode);
        }
    }

    private void appendContentId(ObjectNode objectNode, String zoteroAttachmentDownloadUrl, HashType hashType) throws ContentStreamException {
        try {
            IRI downloadIRI = RefNodeFactory.toIRI(zoteroAttachmentDownloadUrl);
            InputStream attachementInputStream = ContentQueryUtil.getContent(dereferencer, downloadIRI, persisting);
            if (attachementInputStream == null) {
                throw new ContentStreamException("cannot generate Zenodo record due to unresolved attachment [" + zoteroAttachmentDownloadUrl + "]");
            }
            IRI contentId = Hasher.calcHashIRI(
                    attachementInputStream,
                    NullOutputStream.INSTANCE,
                    hashType
            );
            ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.HAS_VERSION, contentId.getIRIString());
        } catch (IOException e) {
            throw new ContentStreamException("cannot generate Zenodo record due to unresolved attachment [" + zoteroAttachmentDownloadUrl + "]", e);
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
        objectNode.put("description", "Uploaded by Plazi for Bat Literature Project. We do not have abstracts.");
        ObjectNode metadata = new ObjectMapper().createObjectNode();
        metadata.set("metadata", objectNode);
        IOUtils.copy(IOUtils.toInputStream(metadata.toString(), StandardCharsets.UTF_8), outputStream);
        IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
        objectNode.removeAll();
        foundAtLeastOne.set(true);
    }


    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
