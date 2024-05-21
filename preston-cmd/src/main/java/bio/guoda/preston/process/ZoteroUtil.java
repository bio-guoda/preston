package bio.guoda.preston.process;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;

public class ZoteroUtil {

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
}
