package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.StatementsListenerEmitterAdapter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;


public class RegistryReaderTaxonWorksTest {

    @Test
    public void parseEmptyCitationArray() throws IOException {
        List<Quad> statements = new ArrayList<Quad>();
        RegistryReaderTaxonWorks.parseCitations(RefNodeFactory.toIRI("https://example.org/"), new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                statements.add(statement);
            }
        }, IOUtils.toInputStream("[]", StandardCharsets.UTF_8), RefNodeFactory.toIRI("https://example.org"));

        assertThat(statements.size(), Is.is(0));

    }

    @Test
    public void parseHandleQuery() throws IOException {
        List<Quad> emitted = new ArrayList<>();

        new RegistryReaderTaxonWorks(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("taxonworks/citations-VVpT9aMPkqtnzmRVUx5jtg.json");
            }
        }, new StatementsListenerEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                emitted.add(statement);
            }

            @Override
            public void on(Quad statement) {
                emitted.add(statement);
            }
        }).on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("https://sfg.taxonworks.org/api/v1/citations/?citation_object_type=BiologicalAssociation&project_token=VVpT9aMPkqtnzmRVUx5jtg"),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("hash://sha256/abc"))
        );

        assertThat(emitted.size(), Is.is(253));

    }

    @Test
    public void handleAssociationsQuery() throws IOException {
        List<Quad> emitted = new ArrayList<>();

        new RegistryReaderTaxonWorks(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("taxonworks/biological-assocations-ZEJhFp9sq8kBfks15qAbAg.json");
            }
        }, new StatementsListenerEmitterAdapter() {
            @Override
            public void emit(Quad statement) {

            }

            @Override
            public void on(Quad statement) {
                emitted.add(statement);
            }
        }).on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("https://sfg.taxonworks.org/api/v1/biological_associations/157457?project_token=ZEJhFp9sq8kBfks15qAbAg"),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("hash://sha256/abc"))
        );

        assertThat(emitted.size(), Is.is(6));

    }

    @Test
    public void handleOTUQuery() throws IOException {
        List<Quad> statements = new ArrayList<>();

        new RegistryReaderTaxonWorks(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("taxonworks/otus-ZEJhFp9sq8kBfks15qAbAg.json");
            }
        }, new StatementsListenerEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                statements.add(statement);
            }

            @Override
            public void on(Quad statement) {
                statements.add(statement);
            }
        }).on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("https://sfg.taxonworks.org/api/v1/otus/41859?project_token=ZEJhFp9sq8kBfks15qAbAg"),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("hash://sha256/abc"))
        );

        assertThat(statements.size(), Is.is(3));


        Quad second = statements.get(0);
        assertThat(second.getSubject().toString(), Is.is("<hash://sha256/abc>"));
        assertThat(second.getPredicate().toString(), Is.is("<http://www.w3.org/ns/prov#hadMember>"));
        assertThat(second.getObject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/taxon_names/372119?project_token=ZEJhFp9sq8kBfks15qAbAg>"));

        Quad third = statements.get(1);
        assertThat(third.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/taxon_names/372119?project_token=ZEJhFp9sq8kBfks15qAbAg>"));
        assertThat(third.getPredicate().toString(), Is.is("<http://purl.org/dc/elements/1.1/format>"));
        assertThat(third.getObject().toString(), Is.is("\"application/json\""));

        Quad forth = statements.get(2);
        assertThat(forth.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/taxon_names/372119?project_token=ZEJhFp9sq8kBfks15qAbAg>"));
        assertThat(forth.getPredicate().toString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(forth.getObject() instanceof BlankNode, Is.is(true));

    }


    @Test
    public void handleTaxonNameQuery() throws IOException {
        List<Quad> statements = new ArrayList<>();

        new RegistryReaderTaxonWorks(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("taxonworks/taxon-names-ZEJhFp9sq8kBfks15qAbAg.json");
            }
        }, new StatementsListenerEmitterAdapter() {
            @Override
            public void emit(Quad statement) {

            }

            @Override
            public void on(Quad statement) {
                statements.add(statement);
            }
        }).on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("https://sfg.taxonworks.org/api/v1/taxon_names/372119?project_token=ZEJhFp9sq8kBfks15qAbAg"),
                RefNodeConstants.HAS_VERSION,
                RefNodeFactory.toIRI("hash://sha256/abc"))
        );

        assertThat(statements.size(), Is.is(3));

        Quad second = statements.get(0);
        assertThat(second.getSubject().toString(), Is.is("<hash://sha256/abc>"));
        assertThat(second.getPredicate().toString(), Is.is("<http://www.w3.org/ns/prov#hadMember>"));
        assertThat(second.getObject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/taxon_names/372113?project_token=ZEJhFp9sq8kBfks15qAbAg>"));

        Quad third = statements.get(1);
        assertThat(third.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/taxon_names/372113?project_token=ZEJhFp9sq8kBfks15qAbAg>"));
        assertThat(third.getPredicate().toString(), Is.is("<http://purl.org/dc/elements/1.1/format>"));
        assertThat(third.getObject().toString(), Is.is("\"application/json\""));

        Quad forth = statements.get(2);
        assertThat(forth.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/taxon_names/372113?project_token=ZEJhFp9sq8kBfks15qAbAg>"));
        assertThat(forth.getPredicate().toString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(forth.getObject() instanceof BlankNode, Is.is(true));


    }

    @Test
    public void parseEmptyCitationObject() throws IOException {
        List<Quad> statements = new ArrayList<>();
        RegistryReaderTaxonWorks.parseCitations(RefNodeFactory.toIRI("https://example.org/"), new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                statements.add(statement);
            }
        }, IOUtils.toInputStream("{}", StandardCharsets.UTF_8), RefNodeFactory.toIRI("https://example.org"));

        assertThat(statements.size(), Is.is(0));

    }

    @Test
    public void parseBiologicalAssociationObject() throws IOException {
        List<Quad> statements = new ArrayList<>();
        RegistryReaderTaxonWorks.parseCitations(RefNodeFactory.toIRI("https://example.org/"), new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                statements.add(statement);
            }
        }, IOUtils.toInputStream("[{\n" +
                "    \"id\": 497877,\n" +
                "    \"citation_object_id\": 54526,\n" +
                "    \"citation_object_type\": \"BiologicalAssociation\",\n" +
                "    \"source_id\": 49000,\n" +
                "    \"pages\": \"7-15,3-5\",\n" +
                "    \"is_original\": null,\n" +
                "    \"created_by_id\": 78,\n" +
                "    \"updated_by_id\": 78,\n" +
                "    \"project_id\": 16,\n" +
                "    \"citation_source_body\": \"Abai, 1976:7-15,3-5\"\n" +
                "  }\n]", StandardCharsets.UTF_8), RefNodeFactory.toIRI("https://example.org"));

        assertThat(statements.size(), Is.is(8));

        Quad first = statements.get(0);
        assertThat(first.getSubject().toString(), Is.is("<https://example.org/>"));
        assertThat(first.getPredicate().toString(), Is.is("<http://www.w3.org/ns/prov#hadMember>"));
        assertThat(first.getObject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/sources/49000?extend[]=bibtex>"));

        Quad second = statements.get(1);
        assertThat(second.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/sources/49000?extend[]=bibtex>"));
        assertThat(second.getPredicate().toString(), Is.is("<http://purl.org/dc/elements/1.1/format>"));
        assertThat(second.getObject().toString(), Is.is("\"application/json\""));

        Quad third = statements.get(2);
        assertThat(third.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/sources/49000?extend[]=bibtex>"));
        assertThat(third.getPredicate().toString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(third.getObject() instanceof BlankNode, Is.is(true));

        Quad forth = statements.get(3);
        assertThat(forth.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/biological_associations/54526?extend[]=biological_relationship>"));
        assertThat(forth.getPredicate().toString(), Is.is("<http://www.w3.org/ns/prov#wasDerivedFrom>"));
        assertThat(forth.getObject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/sources/49000?extend[]=bibtex>"));

        Quad fifth = statements.get(4);
        assertThat(fifth.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/biological_associations/54526?extend[]=biological_relationship>"));
        assertThat(fifth.getPredicate().toString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(fifth.getObject() instanceof BlankNode, Is.is(true));

    }

    @Test
    public void parseCitationObject() throws IOException {
        List<Quad> statements = new ArrayList<>();
        RegistryReaderTaxonWorks.parseCitations(RefNodeFactory.toIRI("https://example.org/"), new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                statements.add(statement);
            }
        }, IOUtils.toInputStream("[{\n" +
                "    \"id\": 497877,\n" +
                "    \"citation_object_id\": 54526,\n" +
                "    \"citation_object_type\": \"BiologicalAssociation\",\n" +
                "    \"source_id\": 49000,\n" +
                "    \"pages\": \"7-15,3-5\",\n" +
                "    \"is_original\": null,\n" +
                "    \"created_by_id\": 78,\n" +
                "    \"updated_by_id\": 78,\n" +
                "    \"project_id\": 16,\n" +
                "    \"citation_source_body\": \"Abai, 1976:7-15,3-5\"\n" +
                "  }\n]", StandardCharsets.UTF_8), RefNodeFactory.toIRI("https://example.org"));

        assertThat(statements.size(), Is.is(8));

        Quad first = statements.get(0);
        assertThat(first.getSubject().toString(), Is.is("<https://example.org/>"));
        assertThat(first.getPredicate().toString(), Is.is("<http://www.w3.org/ns/prov#hadMember>"));
        assertThat(first.getObject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/sources/49000?extend[]=bibtex>"));

        Quad second = statements.get(1);
        assertThat(second.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/sources/49000?extend[]=bibtex>"));
        assertThat(second.getPredicate().toString(), Is.is("<http://purl.org/dc/elements/1.1/format>"));
        assertThat(second.getObject().toString(), Is.is("\"application/json\""));

        Quad third = statements.get(2);
        assertThat(third.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/sources/49000?extend[]=bibtex>"));
        assertThat(third.getPredicate().toString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(third.getObject() instanceof BlankNode, Is.is(true));

        Quad forth = statements.get(3);
        assertThat(forth.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/biological_associations/54526?extend[]=biological_relationship>"));
        assertThat(forth.getPredicate().toString(), Is.is("<http://www.w3.org/ns/prov#wasDerivedFrom>"));
        assertThat(forth.getObject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/sources/49000?extend[]=bibtex>"));

        Quad fifth = statements.get(4);
        assertThat(fifth.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/biological_associations/54526?extend[]=biological_relationship>"));
        assertThat(fifth.getPredicate().toString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(fifth.getObject() instanceof BlankNode, Is.is(true));

    }

    @Test
    public void parseCitationObjectWithProjectToken() throws IOException {
        String projectToken = "ABC123";
        assertAppendProjectToken(projectToken);

    }

    private void assertAppendProjectToken(String projectToken) throws IOException {
        List<Quad> statements = new ArrayList<>();
        RegistryReaderTaxonWorks.parseCitations(RefNodeFactory.toIRI("https://example.org/?project_token=" + projectToken), new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                statements.add(statement);
            }
        }, IOUtils.toInputStream("[{\n" +
                "    \"id\": 497877,\n" +
                "    \"citation_object_id\": 54526,\n" +
                "    \"citation_object_type\": \"BiologicalAssociation\",\n" +
                "    \"source_id\": 49000,\n" +
                "    \"pages\": \"7-15,3-5\",\n" +
                "    \"is_original\": null,\n" +
                "    \"created_by_id\": 78,\n" +
                "    \"updated_by_id\": 78,\n" +
                "    \"project_id\": 16,\n" +
                "    \"citation_source_body\": \"Abai, 1976:7-15,3-5\"\n" +
                "  }\n]", StandardCharsets.UTF_8), RefNodeFactory.toIRI("https://example.org?project_token=" + projectToken));

        assertThat(statements.size(), Is.is(8));

        Quad first = statements.get(0);
        assertThat(first.getSubject().toString(), Is.is("<https://example.org/?project_token=" + projectToken + ">"));
        assertThat(first.getPredicate().toString(), Is.is("<http://www.w3.org/ns/prov#hadMember>"));
        assertThat(first.getObject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/sources/49000?extend[]=bibtex>"));

        Quad second = statements.get(1);
        assertThat(second.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/sources/49000?extend[]=bibtex>"));
        assertThat(second.getPredicate().toString(), Is.is("<http://purl.org/dc/elements/1.1/format>"));
        assertThat(second.getObject().toString(), Is.is("\"application/json\""));

        Quad third = statements.get(2);
        assertThat(third.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/sources/49000?extend[]=bibtex>"));
        assertThat(third.getPredicate().toString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(third.getObject() instanceof BlankNode, Is.is(true));

        Quad forth = statements.get(3);
        assertThat(forth.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/biological_associations/54526?project_token=" + projectToken + "&extend[]=biological_relationship>"));
        assertThat(forth.getPredicate().toString(), Is.is("<http://www.w3.org/ns/prov#wasDerivedFrom>"));
        assertThat(forth.getObject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/sources/49000?extend[]=bibtex>"));

        Quad fifth = statements.get(4);
        assertThat(fifth.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/biological_associations/54526?project_token=" + projectToken + "&extend[]=biological_relationship>"));
        assertThat(fifth.getPredicate().toString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(fifth.getObject() instanceof BlankNode, Is.is(true));
    }

    @Test
    public void parseCitationObjectWithProjectTokenWithHyphen() throws IOException {
        assertAppendProjectToken("Ots0-yen4dVefn0Etyxvgw");
    }

    @Test
    public void parseCitationObjectWithProjectTokenWithUnderScore() throws IOException {
        assertAppendProjectToken("ekMTicbZWijqmdpHKqs_TA");
    }

    @Test
    public void parseProjectIndex() throws IOException {
        List<Quad> statements = new ArrayList<>();
        RegistryReaderTaxonWorks.parseProjectIndex(new StatementsEmitterAdapter() {
                                                       @Override
                                                       public void emit(Quad statement) {
                                                           statements.add(statement);
                                                       }
                                                   }, IOUtils.toInputStream("{\n" +
                        "  \"success\": true,\n" +
                        "  \"open_projects\": [\n" +
                        "    {\n" +
                        "      \"UndtpSwdHsSRw8K3ddsTNQ\": \"Terrestrial Parasite Tracker (TPT)\"\n" +
                        "    },\n" +
                        "    {\n" +
                        "      \"adhBi59dc13U7RxbgNE5HQ\": \"Universal Chalcidoidea Database (UCD)\"\n" +
                        "    }]}", StandardCharsets.UTF_8),
                RefNodeFactory.toIRI("https://example.org"));

        assertThat(statements.size(), Is.is(6));

        Quad first = statements.get(0);
        assertThat(first.getSubject().toString(), Is.is("<https://example.org>"));
        assertThat(first.getPredicate().toString(), Is.is("<http://www.w3.org/ns/prov#hadMember>"));
        assertThat(first.getObject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/citations/?citation_object_type=BiologicalAssociation&project_token=UndtpSwdHsSRw8K3ddsTNQ>"));

        Quad second = statements.get(1);
        assertThat(second.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/citations/?citation_object_type=BiologicalAssociation&project_token=UndtpSwdHsSRw8K3ddsTNQ>"));
        assertThat(second.getPredicate().toString(), Is.is("<http://purl.org/dc/elements/1.1/format>"));
        assertThat(second.getObject().toString(), Is.is("\"application/json\""));

        Quad third = statements.get(2);
        assertThat(third.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/citations/?citation_object_type=BiologicalAssociation&project_token=UndtpSwdHsSRw8K3ddsTNQ>"));
        assertThat(third.getPredicate().toString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(third.getObject() instanceof BlankNode, Is.is(true));

        Quad fourth = statements.get(3);
        assertThat(fourth.getSubject().toString(), Is.is("<https://example.org>"));
        assertThat(fourth.getPredicate().toString(), Is.is("<http://www.w3.org/ns/prov#hadMember>"));
        assertThat(fourth.getObject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/citations/?citation_object_type=BiologicalAssociation&project_token=adhBi59dc13U7RxbgNE5HQ>"));

        Quad fifth = statements.get(4);
        assertThat(fifth.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/citations/?citation_object_type=BiologicalAssociation&project_token=adhBi59dc13U7RxbgNE5HQ>"));
        assertThat(fifth.getPredicate().toString(), Is.is("<http://purl.org/dc/elements/1.1/format>"));
        assertThat(fifth.getObject().toString(), Is.is("\"application/json\""));

        Quad sixth = statements.get(5);
        assertThat(sixth.getSubject().toString(), Is.is("<https://sfg.taxonworks.org/api/v1/citations/?citation_object_type=BiologicalAssociation&project_token=adhBi59dc13U7RxbgNE5HQ>"));
        assertThat(sixth.getPredicate().toString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(sixth.getObject() instanceof BlankNode, Is.is(true));

    }


    @Test
    public void paging() {
        List<Quad> statements = new ArrayList<Quad>();
        RegistryReaderTaxonWorks.emitNextPage(0, 10, new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                statements.add(statement);

            }
        }, "https://example.org");

        assertThat(statements.size(), Is.is(3));

        assertFirstPageSize10(statements);
    }

    @Test
    public void doNotEmitNextPageRequestOnEmptyResults() throws JsonProcessingException {
        List<Quad> statements = new ArrayList<Quad>();
        RegistryReaderTaxonWorks.emitNextPageIfNeeded(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                statements.add(statement);

            }
        }, RefNodeFactory.toIRI("https://example.org"), new ObjectMapper().readTree("[]"));

        assertThat(statements.size(), Is.is(0));

    }

    @Test
    public void doEmitNextPageRequestOnNonEmptyResults() throws JsonProcessingException {
        List<Quad> statements = new ArrayList<Quad>();
        RegistryReaderTaxonWorks.emitNextPageIfNeeded(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                statements.add(statement);

            }
        }, RefNodeFactory.toIRI("https://example.org"), new ObjectMapper().readTree("[{}, {}]"));

        assertThat(statements.size(), Is.is(3));

        Quad first = statements.get(0);
        assertThat(first.getSubject().toString(), Is.is("<https://example.org?page=1&per=2>"));
        assertThat(first.getPredicate().toString(), Is.is("<http://purl.org/pav/createdBy>"));
        assertThat(first.getObject().toString(), Is.is("<https://sfg.taxonworks.org>"));

        Quad second = statements.get(1);
        assertThat(second.getSubject().toString(), Is.is("<https://example.org?page=1&per=2>"));
        assertThat(second.getPredicate().toString(), Is.is("<http://purl.org/dc/elements/1.1/format>"));
        assertThat(second.getObject().toString(), Is.is("\"application/json\""));

        Quad third = statements.get(2);
        assertThat(third.getSubject().toString(), Is.is("<https://example.org?page=1&per=2>"));
        assertThat(third.getPredicate().toString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(third.getObject() instanceof BlankNode, Is.is(true));
    }

    @Test
    public void doEmitNextPageRequestOnNonEmptyResultsExplicitPage() throws JsonProcessingException {
        List<Quad> statements = new ArrayList<Quad>();
        RegistryReaderTaxonWorks.emitNextPageIfNeeded(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                statements.add(statement);

            }
        }, RefNodeFactory.toIRI("https://example.org?page=3&per=2"), new ObjectMapper().readTree("[{}, {}]"));

        assertThat(statements.size(), Is.is(3));

        Quad first = statements.get(0);
        assertThat(first.getSubject().toString(), Is.is("<https://example.org?page=4&per=2>"));
        assertThat(first.getPredicate().toString(), Is.is("<http://purl.org/pav/createdBy>"));
        assertThat(first.getObject().toString(), Is.is("<https://sfg.taxonworks.org>"));

        Quad second = statements.get(1);
        assertThat(second.getSubject().toString(), Is.is("<https://example.org?page=4&per=2>"));
        assertThat(second.getPredicate().toString(), Is.is("<http://purl.org/dc/elements/1.1/format>"));
        assertThat(second.getObject().toString(), Is.is("\"application/json\""));

        Quad third = statements.get(2);
        assertThat(third.getSubject().toString(), Is.is("<https://example.org?page=4&per=2>"));
        assertThat(third.getPredicate().toString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(third.getObject() instanceof BlankNode, Is.is(true));


    }

    @Test
    public void pagingImplicit() {
        List<Quad> statements = new ArrayList<Quad>();
        RegistryReaderTaxonWorks.emitNextPage(0, 10, new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                statements.add(statement);

            }
        }, "https://example.org");

        assertThat(statements.size(), Is.is(3));

        assertFirstPageSize10(statements);
    }

    private void assertFirstPageSize10(List<Quad> statements) {
        Quad first = statements.get(0);
        assertThat(first.getSubject().toString(), Is.is("<https://example.org?page=0&per=10>"));
        assertThat(first.getPredicate().toString(), Is.is("<http://purl.org/pav/createdBy>"));
        assertThat(first.getObject().toString(), Is.is("<https://sfg.taxonworks.org>"));

        Quad second = statements.get(1);
        assertThat(second.getSubject().toString(), Is.is("<https://example.org?page=0&per=10>"));
        assertThat(second.getPredicate().toString(), Is.is("<http://purl.org/dc/elements/1.1/format>"));
        assertThat(second.getObject().toString(), Is.is("\"application/json\""));

        Quad third = statements.get(2);
        assertThat(third.getSubject().toString(), Is.is("<https://example.org?page=0&per=10>"));
        assertThat(third.getPredicate().toString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(third.getObject() instanceof BlankNode, Is.is(true));
    }

}