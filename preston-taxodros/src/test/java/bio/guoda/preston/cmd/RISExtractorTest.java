package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RISExtractorTest {

    public static final Pattern RIS_KEY_VALUE = Pattern.compile("[^A-Z]*(?<key>[A-Z][A-Z2])[ ]+-(?<value>.*)");
    public static final Pattern BHL_PART_URL = Pattern.compile("(?<prefix>https://www.biodiversitylibrary.org/part/)(?<part>[0-9]+)");
    public static final String TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    @Test
    public void streamRISToZenodoLineJson() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<JsonNode>();

        Consumer<JsonNode> listener = new Consumer<JsonNode>() {
            @Override
            public void accept(JsonNode jsonNode) {
                jsonObjects.add(translateRISToZenodo(jsonNode));
            }
        };

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-multiple.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener);


        assertThat(jsonObjects.size(), is(5));

        JsonNode rootNode = jsonObjects.get(1);
        JsonNode taxonNode = rootNode.get("metadata");

        assertThat(taxonNode.get("description").asText(), is("(Uploaded by Plazi from the Biodiversity Heritage Library) No abstract provided."));

        JsonNode communities = taxonNode.get("communities");
        assertThat(communities.isArray(), is(true));
        assertThat(communities.size(), is(1));
        assertThat(communities.get(0).get("identifier").asText(), is("biosyslit"));


        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("line:foo:bar!/L23-L44"));
        assertThat(taxonNode.get(TYPE).asText(), is("application/x-research-info-systems"));
        assertThat(taxonNode.get("referenceId").asText(), is("https://www.biodiversitylibrary.org/part/337600"));

        JsonNode creators = taxonNode.get("creators");
        assertThat(creators.isArray(), is(true));
        assertThat(creators.size(), is(5));
        assertThat(creators.get(0).get("name").asText(), is("Le Cesne, Maxime"));
        assertThat(creators.get(1).get("name").asText(), is("Bourgoin, Thierry,"));
        assertThat(creators.get(2).get("name").asText(), is("Hoch, Hannelore"));
        assertThat(creators.get(3).get("name").asText(), is("Luo, Yang"));
        assertThat(creators.get(4).get("name").asText(), is("Zhang, Yalin"));
        assertThat(taxonNode.get("journal_title").asText(), is("Subterranean Biology"));
        assertThat(taxonNode.get("journal_volume").asText(), is("43"));
        assertThat(taxonNode.get("journal_issue"), is(nullValue()));
        assertThat(taxonNode.get("journal_pages").asText(), is("145-168"));
        assertThat(taxonNode.get("publication_date").asText(), is("2022"));
        assertThat(taxonNode.get("access_right"), is(nullValue()));
        assertThat(taxonNode.get("publication_type").textValue(), is("article"));
        assertThat(taxonNode.get("upload_type").textValue(), is("publication"));
        assertThat(taxonNode.get("doi").textValue(), is("10.3897/subtbiol.43.85804"));
        assertThat(taxonNode.get("filename").textValue(), is("https://www.biodiversitylibrary.org/partpdf/337600"));

        JsonNode keywords = taxonNode.at("/keywords");
        assertThat(keywords.get(0).asText(), is("cave"));

        JsonNode identifiers = taxonNode.at("/related_identifiers");
        assertThat(identifiers.size(), is(4));
        // provided by Zoteros
        assertThat(identifiers.get(0).get("relation").asText(), is("wasDerivedFrom"));
        assertThat(identifiers.get(0).get("identifier").asText(), is("line:foo:bar!/L23-L44"));
        assertThat(identifiers.get(1).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(1).get("identifier").asText(), is("10.3897/subtbiol.43.85804"));

        // calculated on the fly
        assertThat(identifiers.get(2).get("relation").asText(), is("wasDerivedFrom"));
        assertThat(identifiers.get(2).get("identifier").asText(), is("https://www.biodiversitylibrary.org/part/337600"));
        assertThat(identifiers.get(3).get("relation").asText(), is("wasDerivedFrom"));
        assertThat(identifiers.get(3).get("identifier").asText(), is("https://www.biodiversitylibrary.org/partpdf/337600"));
        assertThat(identifiers.get(3).has("resource_type"), is(false));
    }

    @Test
    public void streamSingleRIS() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<JsonNode>();

        Consumer<JsonNode> listener = new Consumer<JsonNode>() {
            @Override
            public void accept(JsonNode jsonNode) {
                jsonObjects.add(translateRISToZenodo(jsonNode));
            }
        };

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-single.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener);


        assertThat(jsonObjects.size(), is(1));
    }

    @Test
    public void streamPartialRIS() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<JsonNode>();

        Consumer<JsonNode> listener = new Consumer<JsonNode>() {
            @Override
            public void accept(JsonNode jsonNode) {
                jsonObjects.add(translateRISToZenodo(jsonNode));
            }
        };

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-partial.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener);


        assertThat(jsonObjects.size(), is(1));
    }

    public static void parseRIS(InputStream inputStream, Consumer<JsonNode> listener) throws IOException {
        BufferedReader bufferedReader = IOUtils.toBufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

        String line = null;
        ObjectNode record = null;
        long recordStart = -1;

        long lineNumber = 0;
        while ((line = bufferedReader.readLine()) != null) {
            lineNumber++;
            Matcher matcher = RIS_KEY_VALUE.matcher(line);
            if (matcher.matches()) {
                String key = matcher.group("key");
                String value = StringUtils.trim(matcher.group("value"));
                if ("TY".equals(key)) {
                    record = new ObjectMapper().createObjectNode();
                    record.put(key, value);
                    recordStart = lineNumber;
                } else if (record != null && recordStart > -1) {
                    if ("ER".equals(key)) {
                        record.put(RefNodeConstants.WAS_DERIVED_FROM.getIRIString(), "line:foo:bar!/L" + recordStart + "-L" + lineNumber);
                        record.put(TYPE, "application/x-research-info-systems");
                        listener.accept(record);
                        record = null;
                        recordStart = -1;
                    } else {
                        JsonNode jsonNode = record.get(key);
                        if (jsonNode == null) {
                            record.put(key, value);
                        } else {
                            ArrayNode array = null;
                            if (jsonNode.isArray()) {
                                array = (ArrayNode) jsonNode;
                                array.add(value);
                            } else {
                                array = new ObjectMapper().createArrayNode().add(jsonNode.asText()).add(value);
                            }
                            record.set(key, array);
                        }
                    }
                }
            }
        }
    }

    private ObjectNode translateRISToZenodo(JsonNode jsonNode) {
        ObjectNode zenodoObject = new ObjectMapper().createObjectNode();
        ObjectNode metadata = new ObjectMapper().createObjectNode();
        ArrayNode relatedIdentifiers = new ObjectMapper().createArrayNode();
        metadata.put("description", "(Uploaded by Plazi from the Biodiversity Heritage Library) No abstract provided.");
        metadata.set("communities",
                new ObjectMapper()
                        .createArrayNode()
                        .add(new ObjectMapper().createObjectNode().put("identifier", "biosyslit"))
        );
        if (jsonNode.has(RefNodeConstants.WAS_DERIVED_FROM.getIRIString())) {
            String recordLocation = jsonNode.get(RefNodeConstants.WAS_DERIVED_FROM.getIRIString()).asText();
            addDerivedFrom(relatedIdentifiers, recordLocation);
            metadata.put(RefNodeConstants.WAS_DERIVED_FROM.getIRIString(), recordLocation);
        }

        if (jsonNode.has(TYPE)) {
            metadata.put(TYPE, jsonNode.get(TYPE).asText());
        }
        if (jsonNode.has("TI")) {
            metadata.put("title", jsonNode.get("TI").asText());
        }
        if (jsonNode.has("T2")) {
            metadata.put("journal_title", jsonNode.get("T2").asText());
        }

        if (jsonNode.has(("VL"))) {
            metadata.put("journal_volume", jsonNode.get("VL").asText());
        }
        if (jsonNode.has("SP") && jsonNode.has("EP")) {
            metadata.put("journal_pages", jsonNode.get("SP").asText() + "-" + jsonNode.get("EP").asText());
        }
        if (jsonNode.has("PY")) {
            metadata.put("publication_date", jsonNode.get("PY").asText());

        }
        if (jsonNode.has("TY")) {
            if (StringUtils.equals(jsonNode.get("TY").asText(), "JOUR")) {
                metadata.put("publication_type", "article");
                metadata.put("upload_type", "publication");
            }

        }
        if (jsonNode.has("DO")) {
            String doiString = jsonNode.get("DO").asText();
            metadata.put("doi", doiString);
            addAlternateIdentifier(relatedIdentifiers, doiString);
        }


        if (jsonNode.has("UR")) {
            String url = jsonNode.get("UR").asText();
            metadata.put("referenceId", url);
            addDerivedFrom(relatedIdentifiers, url);
            Matcher matcher = BHL_PART_URL.matcher(url);
            if (matcher.matches()) {
                String s = "https://www.biodiversitylibrary.org/partpdf/" + matcher.group("part");
                metadata.put("filename", s);
                addDerivedFrom(relatedIdentifiers, s);
            }
        }

        JsonNode authors = jsonNode.get("AU");
        if (authors != null) {
            ArrayNode creators = new ObjectMapper().createArrayNode();
            if (authors.isArray()) {
                authors.forEach(value -> creators.add(new ObjectMapper().createObjectNode().put("name", value.asText())));
            }
            metadata.set("creators", creators);
        }
        JsonNode keywords = jsonNode.get("KW");
        if (keywords != null) {
            ArrayNode creators = new ObjectMapper().createArrayNode();
            if (keywords.isArray()) {
                keywords.forEach(value -> creators.add(value.asText()));
            }
            metadata.set("keywords", creators);
        }

        metadata.set("related_identifiers", relatedIdentifiers);
        zenodoObject.set("metadata", metadata);
        return zenodoObject;
    }

    private ArrayNode addDerivedFrom(ArrayNode relatedIdentifiers, String s) {
        return addRelation(relatedIdentifiers, s, "wasDerivedFrom");
    }

    private ArrayNode addAlternateIdentifier(ArrayNode relatedIdentifiers, String s) {
        return addRelation(relatedIdentifiers, s, "isAlternateIdentifier");
    }

    private ArrayNode addRelation(ArrayNode relatedIdentifiers, String s, String relationType) {
        return relatedIdentifiers.add(new ObjectMapper().createObjectNode().put("relation", relationType).put("identifier", s));
    }

}
