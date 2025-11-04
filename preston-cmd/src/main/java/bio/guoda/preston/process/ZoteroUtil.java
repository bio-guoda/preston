package bio.guoda.preston.process;

import bio.guoda.preston.cmd.ZenodoMetaUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZoteroUtil {

    public static final String ZOTERO_BOOK_SECTION = "bookSection";
    public static final String ZOTERO_PREPRINT = "preprint";
    public static final String ZOTERO_REPORT = "report";
    public static final String ZOTERO_THESIS = "thesis";
    public static final String ZOTERO_CONFERENCE_PAPER = "conferencePaper";

    public static final Map<String, String> ZOTERO_TO_RIS_PUB_TYPE_TRANSLATION_TABLE = new TreeMap<String, String>() {{
        put(ZOTERO_JOURNAL_ARTICLE, ZenodoMetaUtil.PUBLICATION_TYPE_ARTICLE);
        put(ZOTERO_BOOK, "BOOK");
        put(ZOTERO_BOOK_SECTION, "CHAP");
        put(ZOTERO_REPORT, "RPRT");
        put(ZOTERO_THESIS, "THES");
        put(ZOTERO_CONFERENCE_PAPER, "CPAPER");
    }};

    public static final Map<String, String> ZOTERO_TO_ZENODO_PUB_TYPE_TRANSLATION_TABLE = new TreeMap<String, String>() {{
        put(ZOTERO_JOURNAL_ARTICLE, ZenodoMetaUtil.PUBLICATION_TYPE_ARTICLE);
        put(ZOTERO_BOOK, ZenodoMetaUtil.PUBLICATION_TYPE_BOOK);
        put(ZOTERO_BOOK_SECTION, ZenodoMetaUtil.PUBLICATION_TYPE_BOOK_SECTION);
        put(ZOTERO_PREPRINT, ZenodoMetaUtil.PUBLICATION_TYPE_PREPRINT);
        put(ZOTERO_REPORT, ZenodoMetaUtil.PUBLICATION_TYPE_REPORT);
        put(ZOTERO_THESIS, ZenodoMetaUtil.PUBLICATION_TYPE_THESIS);
        put(ZOTERO_CONFERENCE_PAPER, ZenodoMetaUtil.PUBLICATION_TYPE_CONFERENCE_PAPER);
    }};
    public static final String ZOTERO_JOURNAL_ARTICLE = "journalArticle";
    public static final String ZOTERO_BOOK = "book";

    public static String getAttachmentDownloadUrl(JsonNode zoteroRecord) {
        return getAttachmentProperty(zoteroRecord, "/links/enclosure/href");
    }

    public static String getAttachmentProperty(JsonNode zoteroRecord, String jsonPtrExpr) {
        String zoteroAttachmentDownloadUrl = null;
        String itemType = zoteroRecord.at("/data/itemType").asText();
        if (StringUtils.equals(itemType, "attachment")) {
            zoteroAttachmentDownloadUrl = zoteroRecord.at(jsonPtrExpr).asText();
        }
        return zoteroAttachmentDownloadUrl;
    }

    public static String getAttachmentType(JsonNode zoteroRecord) {
        return getAttachmentProperty(zoteroRecord, "/data/contentType");

    }

    public static String getDOI(JsonNode jsonNode) {
        return jsonNode.at("/data/DOI").asText();
    }

    public static String getBookTitle(JsonNode jsonNode) {
        return jsonNode.at("/data/bookTitle").asText();
    }

    public static String getPublisherName(JsonNode jsonNode) {
        return jsonNode.at("/data/publisher").asText();
    }

    public static String getJournalPages(JsonNode jsonNode) {
        return jsonNode.at("/data/pages").asText();
    }

    public static String getJournalIssue(JsonNode jsonNode) {
        return jsonNode.at("/data/issue").asText();
    }

    public static String getJournalVolume(JsonNode jsonNode) {
        return jsonNode.at("/data/volume").asText();
    }

    public static String getJournalTitle(JsonNode jsonNode) {
        return jsonNode.at("/data/publicationTitle").asText();
    }

    public static String getTitle(JsonNode jsonNode) {
        return jsonNode.at("/data/title").asText();
    }

    public static List<String> parseKeywords(JsonNode jsonNode) {
        JsonNode tags = jsonNode.at("/data/tags");
        List<String> tagValues = new ArrayList<>();
        if (!tags.isMissingNode() && tags.isArray()) {
            tags.forEach(t -> {
                if (t.has("tag")) {
                    String tagValue = t.get("tag").asText();
                    tagValues.add(tagValue);
                }
            });
        }
        return tagValues;
    }

    public static String getPublicationDate(JsonNode jsonNode) {
        String dateString = jsonNode.at("/data/date").asText();
        return parseDate(dateString);
    }

    public static String getAbstract(JsonNode jsonNode) {
        return jsonNode.at("/data/abstractNote").textValue();
    }

    public static List<String> parseCreators(JsonNode creators) {
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

    public static String parseDate(String publicationDate) {
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
}
