package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.ProcessorStateAlwaysContinue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static bio.guoda.preston.cmd.RISUtil.TYPE;
import static bio.guoda.preston.cmd.RISUtil.translateRISToZenodo;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RISUtilTest {

    public static void parseRIS(InputStream inputStream, Consumer<ObjectNode> listener, String sourceIRIString) throws IOException {
        RISUtil.parseRIS(inputStream, listener, sourceIRIString, new ProcessorStateAlwaysContinue());
    }


    @Test
    public void streamRISStopProcessing() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<JsonNode>();

        Consumer<ObjectNode> listener = jsonNode -> jsonObjects.add(translateRISToZenodo(jsonNode, Arrays.asList("community-foo"), shouldUseProvidedDoi()));

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-multiple.ris");

        assertNotNull(bibTex);

        RISUtil.parseRIS(bibTex, listener, "foo:bar", new ProcessorState() {

            @Override
            public boolean shouldKeepProcessing() {
                return false;
            }

            @Override
            public void stopProcessing() {

            }
        });

        assertThat(jsonObjects.size(), is(0));

    }

    @Test
    public void streamRISToZenodoLineJson() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<JsonNode>();

        Consumer<ObjectNode> listener = jsonNode -> jsonObjects.add(translateRISToZenodo(jsonNode, Arrays.asList("community-foo"), shouldUseProvidedDoi()));

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-multiple.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener, "foo:bar");


        assertThat(jsonObjects.size(), is(5));

        JsonNode taxonNode = jsonObjects.get(1);

        assertThat(taxonNode.get("description").asText(), is("(Uploaded by Plazi from the Biodiversity Heritage Library) No abstract provided."));

        JsonNode communities = taxonNode.get("communities");
        assertThat(communities.isArray(), is(true));
        assertThat(communities.size(), is(1));
        assertThat(communities.get(0).get("identifier").asText(), is("community-foo"));


        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("https://linker.bio/line:foo:bar!/L23-L44"));
        assertThat(taxonNode.get(TYPE).asText(), is("application/x-research-info-systems"));
        assertThat(taxonNode.get("referenceId").asText(), is("https://www.biodiversitylibrary.org/part/337600"));

        JsonNode creators = taxonNode.get("creators");
        assertThat(creators.isArray(), is(true));
        assertThat(creators.size(), is(5));
        assertThat(creators.get(0).get("name").asText(), is("Le Cesne, Maxime"));
        assertThat(creators.get(1).get("name").asText(), is("Bourgoin, Thierry"));
        assertThat(creators.get(2).get("name").asText(), is("Hoch, Hannelore"));
        assertThat(creators.get(3).get("name").asText(), is("Luo, Yang"));
        assertThat(creators.get(4).get("name").asText(), is("Zhang, Yalin"));
        assertThat(taxonNode.get("title").asText(), is("Coframalaxius bletteryi gen. et sp. nov. from subterranean habitat in Southern France (Hemiptera, Fulgoromorpha, Cixiidae, Oecleini)"));
        assertThat(taxonNode.get("journal_title").asText(), is("Subterranean Biology"));
        assertThat(taxonNode.get("journal_volume").asText(), is("43"));
        assertThat(taxonNode.get("journal_issue"), is(nullValue()));
        assertThat(taxonNode.get("journal_pages").asText(), is("145-168"));
        assertThat(taxonNode.get("publication_date").asText(), is("2022"));
        assertThat(taxonNode.get("access_right"), is(nullValue()));
        assertThat(taxonNode.get("publication_type").textValue(), is("article"));
        assertThat(taxonNode.get("upload_type").textValue(), is("publication"));
        assertThat(taxonNode.get("doi"), is(nullValue()));
        assertThat(taxonNode.get("filename").textValue(), is("bhlpart337600.pdf"));

        List<String> keywordList = RISFileExtractorTest.getKeywordList(taxonNode);
        assertThat(keywordList, hasItem("cave"));

        JsonNode identifiers = taxonNode.at("/related_identifiers");
        assertThat(identifiers.size(), is(4));
        // provided by Zoteros
        assertThat(identifiers.get(0).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(0).get("identifier").asText(), is("https://linker.bio/line:foo:bar!/L23-L44"));
        assertThat(identifiers.get(1).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(1).get("identifier").asText(), is("10.3897/subtbiol.43.85804"));

        assertThat(identifiers.get(2).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(2).get("identifier").asText(), is("https://www.biodiversitylibrary.org/part/337600"));
        assertThat(identifiers.get(2).has("resource_type"), is(false));

        assertThat(identifiers.get(3).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(3).get("identifier").asText(), is("urn:lsid:biodiversitylibrary.org:part:337600"));
        assertThat(identifiers.get(3).has("resource_type"), is(false));

    }
    @Test
    public void streamRISToZenodoPatchedDateRange() throws IOException {
        // see https://github.com/bio-guoda/preston/issues/350
        List<JsonNode> jsonObjects = new ArrayList<JsonNode>();

        Consumer<ObjectNode> listener = jsonNode -> jsonObjects.add(translateRISToZenodo(jsonNode, Arrays.asList("community-foo"), shouldUseProvidedDoi()));

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-date-range.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener, "foo:bar");


        assertThat(jsonObjects.size(), is(1));

        JsonNode taxonNode = jsonObjects.get(0);

        assertThat(taxonNode.get("description").asText(), is("(Uploaded by Plazi from the Biodiversity Heritage Library) No abstract provided."));

        JsonNode communities = taxonNode.get("communities");
        assertThat(communities.isArray(), is(true));
        assertThat(communities.size(), is(1));
        assertThat(communities.get(0).get("identifier").asText(), is("community-foo"));


        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("https://linker.bio/line:foo:bar!/L1-L14"));
        assertThat(taxonNode.get(TYPE).asText(), is("application/x-research-info-systems"));
        assertThat(taxonNode.get("referenceId").asText(), is("https://www.biodiversitylibrary.org/part/375075"));

        assertThat(taxonNode.get("publication_date").asText(), is("2004/2005"));

    }

    @Test
    public void streamRISToZenodoLineJsonWithOriginalDOI() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<>();

        Consumer<ObjectNode> listener = jsonNode
                -> jsonObjects.add(
                translateRISToZenodo(
                        jsonNode,
                        Arrays.asList("community-foo"),
                        true
                )
        );

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-multiple.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener, "foo:bar");


        assertThat(jsonObjects.size(), is(5));

        JsonNode taxonNode = jsonObjects.get(1);

        assertThat(taxonNode.get("description").asText(), is("(Uploaded by Plazi from the Biodiversity Heritage Library) No abstract provided."));

        JsonNode communities = taxonNode.get("communities");
        assertThat(communities.isArray(), is(true));
        assertThat(communities.size(), is(1));
        assertThat(communities.get(0).get("identifier").asText(), is("community-foo"));


        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("https://linker.bio/line:foo:bar!/L23-L44"));
        assertThat(taxonNode.get(TYPE).asText(), is("application/x-research-info-systems"));
        assertThat(taxonNode.get("referenceId").asText(), is("https://www.biodiversitylibrary.org/part/337600"));

        assertThat(taxonNode.get("doi").asText(), is("10.3897/subtbiol.43.85804"));

    }

    private boolean shouldUseProvidedDoi() {
        return false;
    }

    @Test
    public void streamSingleRIS() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<JsonNode>();

        Consumer<ObjectNode> listener = jsonNode -> jsonObjects.add(translateRISToZenodo(jsonNode, Arrays.asList("biosyslit"), shouldUseProvidedDoi()));

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-single.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener, "foo:bar");


        assertThat(jsonObjects.size(), is(1));
    }

    @Test
    public void streamSingleRISNoAuthor() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<JsonNode>();

        Consumer<ObjectNode> listener = jsonNode -> jsonObjects.add(translateRISToZenodo(jsonNode, Arrays.asList("biosyslit"), shouldUseProvidedDoi()));

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-single-no-author.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener, "foo:bar");

        assertThat(jsonObjects.size(), is(1));


        JsonNode jsonNode = jsonObjects.get(0);

        JsonNode creators = jsonNode.at("/creators");

        assertThat(creators.isArray(), is(true));

        assertThat(creators.size(), is(1));
        assertThat(creators.at("/0/name").asText(), is("NA"));
    }

    @Test
    public void streamSingleSSNArticle() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<JsonNode>();

        Consumer<ObjectNode> listener = jsonNode -> jsonObjects.add(translateRISToZenodo(jsonNode, Arrays.asList("biosyslit"), shouldUseProvidedDoi()));

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-issn-article.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener, "foo:bar");


        assertThat(jsonObjects.size(), is(1));
        JsonNode relatedIdentifiers = jsonObjects.get(0).get("related_identifiers");
        assertThat(relatedIdentifiers.size(), is(4));

        assertThat(relatedIdentifiers.get(1).get("identifier").asText(), is("0196-0768"));
        assertThat(relatedIdentifiers.get(1).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(relatedIdentifiers.get(1).get("scheme").asText(), is("issn"));
    }

    @Test
    public void streamSingleSSNChapter() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<JsonNode>();

        Consumer<ObjectNode> listener = jsonNode -> jsonObjects.add(translateRISToZenodo(jsonNode, Arrays.asList("biosyslit"), shouldUseProvidedDoi()));

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-issn-chapter.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener, "foo:bar");


        assertThat(jsonObjects.size(), is(1));
        JsonNode citationRecord = jsonObjects.get(0);

        assertThat(citationRecord.get("upload_type").textValue(), is("publication"));
        assertThat(citationRecord.get("publication_type").textValue(), is("section"));
        assertThat(citationRecord.get("imprint_publisher").textValue(), is("Smithsonian Institution Press"));

        JsonNode relatedIdentifiers = citationRecord.get("related_identifiers");

        assertThat(relatedIdentifiers.size(), is(4));

        assertThat(relatedIdentifiers.get(1).get("identifier").asText(), is("0081-0266"));
        assertThat(relatedIdentifiers.get(1).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(relatedIdentifiers.get(1).get("scheme").asText(), is("issn"));
    }

    @Test

    public void streamSingleISBNChapter() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<JsonNode>();

        Consumer<ObjectNode> listener = jsonNode -> jsonObjects.add(translateRISToZenodo(jsonNode, Arrays.asList("biosyslit"), shouldUseProvidedDoi()));

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-isbn-chapter.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener, "foo:bar");


        assertThat(jsonObjects.size(), is(1));
        JsonNode citationRecord = jsonObjects.get(0);

        assertThat(citationRecord.get("upload_type").textValue(), is("publication"));
        assertThat(citationRecord.get("publication_type").textValue(), is("section"));
        assertThat(citationRecord.get("imprint_publisher").textValue(), is("The Field Museum, Science and Education"));

        JsonNode relatedIdentifiers = citationRecord.get("related_identifiers");

        assertThat(relatedIdentifiers.size(), is(4));

        assertThat(relatedIdentifiers.get(1).get("identifier").asText(), is("9780982841945"));
        assertThat(relatedIdentifiers.get(1).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(relatedIdentifiers.get(1).get("scheme").asText(), is("isbn"));
    }

    @Test
    public void streamSingleBookNoPublisher() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<>();

        Consumer<ObjectNode> listener = jsonNode -> jsonObjects.add(translateRISToZenodo(jsonNode, Arrays.asList("biosyslit"), shouldUseProvidedDoi()));

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-book.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener, "foo:bar");


        assertThat(jsonObjects.size(), is(1));

        JsonNode citationRecord = jsonObjects.get(0);
        assertThat(citationRecord.get("upload_type").textValue(), is("publication"));
        assertThat(citationRecord.get("publication_type").textValue(), is("other"));
        assertThat(citationRecord.get("imprint_publisher"), is(nullValue()));

    }

    @Test
    public void streamSingleBookWithPublisher() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<JsonNode>();

        Consumer<ObjectNode> listener = jsonNode -> jsonObjects.add(translateRISToZenodo(jsonNode, Arrays.asList("biosyslit"), shouldUseProvidedDoi()));

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-book-publisher.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener, "foo:bar");


        assertThat(jsonObjects.size(), is(1));

        JsonNode citationRecord = jsonObjects.get(0);
        assertThat(citationRecord.get("upload_type").textValue(), is("publication"));
        assertThat(citationRecord.get("publication_type").textValue(), is("book"));
        assertThat(citationRecord.get("imprint_publisher").textValue(), is("Field Naturalists Club of Victoria"));

    }

    @Test
    public void streamSingleBookWithCPaper() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<>();

        Consumer<ObjectNode> listener = jsonNode -> jsonObjects.add(translateRISToZenodo(jsonNode, Arrays.asList("biosyslit"), shouldUseProvidedDoi()));

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-cpaper.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener, "foo:bar");


        assertThat(jsonObjects.size(), is(1));

        JsonNode citationRecord = jsonObjects.get(0);
        assertThat(citationRecord.get("upload_type").textValue(), is("publication"));
        assertThat(citationRecord.get("publication_type").textValue(), is("book"));
        assertThat(citationRecord.get("imprint_publisher").textValue(), is("British Ornithologists' Club"));

    }

    @Test
    public void streamSinglePart3743() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<JsonNode>();

        Consumer<ObjectNode> listener = jsonNode -> jsonObjects.add(translateRISToZenodo(jsonNode, Arrays.asList("biosyslit"), shouldUseProvidedDoi()));

        InputStream recordStream = getClass().getResourceAsStream("ris/bhlpart-3743.ris");

        assertNotNull(recordStream);

        parseRIS(recordStream, listener, "foo:bar");


        assertThat(jsonObjects.size(), is(1));
        assertThat(jsonObjects.get(0).at("/creators").size(), is(1));
        assertThat(jsonObjects.get(0).at("/creators/0/name").asText(), is("Pictet, Camille"));
    }

    @Test
    public void streamPartialRIS() throws IOException {

        List<JsonNode> jsonObjects = new ArrayList<JsonNode>();

        Consumer<ObjectNode> listener = new Consumer<ObjectNode>() {
            @Override
            public void accept(ObjectNode jsonNode) {
                jsonObjects.add(translateRISToZenodo(jsonNode, Arrays.asList("community-foo"), shouldUseProvidedDoi()));
            }
        };

        InputStream bibTex = getClass().getResourceAsStream("ris/bhlpart-partial.ris");

        assertNotNull(bibTex);

        parseRIS(bibTex, listener, "foo:bar");


        assertThat(jsonObjects.size(), is(1));
    }


}
