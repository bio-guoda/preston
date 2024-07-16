package bio.guoda.preston.cmd;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static bio.guoda.preston.cmd.RISUtil.TYPE;
import static bio.guoda.preston.cmd.RISUtil.parseRIS;
import static bio.guoda.preston.cmd.RISUtil.translateRISToZenodo;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RISUtilTest {

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


}
