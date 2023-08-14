package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorStateAlwaysContinue;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toLiteral;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;

public class TaxonWorksJSONExtractorTest {

    @Test
    public void buildBiologicalInteractionRecord() throws IOException {
        LinkedList<String> queue = Stream.of(
                "citation.json",
                "source.json",
                "association.json",
                "subject_otu.json",
                "object_otu.json",
                "subject_name.json",
                "object_name.json",
                "subject_name_parent.json",
                "object_name_parent.json"
        )
                .map(x -> "/bio/guoda/preston/process/taxonworks/association-graph/" + x)
                .collect(Collectors.toCollection(LinkedList::new));

        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                return queue.isEmpty()
                        ? null
                        : getClass().getResourceAsStream(queue.pop());
            }
        };


        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        TaxonWorksJSONExtractor extractor = new TaxonWorksJSONExtractor(
                new ProcessorStateAlwaysContinue(),
                blobStore,
                byteArrayOutputStream
        );

        List<String> endpoints = Stream.of(
                "/citations/?citation_object_type=BiologicalAssociation&",
                "/sources/11554?",
                "/biological_associations/5430?",
                "/otus/47016?",
                "/otus/81982?",
                "/taxon_names/377136?",
                "/taxon_names/443429?",
                "/taxon_names/376996?",
                "/taxon_names/443293?"
        )
                .map(x -> "https://sfg.taxonworks.org/api/v1" + x)
                .map(x -> x + "project_token=ZEJhFp9sq8kBfks15qAbAg")
                .collect(Collectors.toList());


        endpoints.stream().flatMap(endpoint ->
                Stream.of(
                        toStatement(
                                toIRI(endpoint),
                                HAS_FORMAT,
                                toLiteral("\"application/json\"")
                        ), toStatement(
                                toIRI(endpoint),
                                HAS_VERSION,
                                toIRI("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1")
                        ))).forEach(extractor::on);

        String actual = IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );

        assertThat(actual, not(isEmptyString()));
        JsonNode jsonNode = new ObjectMapper().readTree(actual);

        assertThat(jsonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(),
                is("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));
        assertThat(jsonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(),
                is("application/vnd.taxonworks+json"));
        assertThat(jsonNode.get("interactionTypeId").asText(),
                is("gid://taxon-works/BiologicalRelationship/2"));
        assertThat(jsonNode.get("referenceId").asText(),
                is("gid://taxon-works/Source::Bibtex/11554"));
        assertThat(jsonNode.get("referenceCitation").asText(),
                is("Ackerman, A.J. (1919a) Two leafhoppers injurious to apple nursery stock. <i>Bulletin. United States Department of Agriculture. Washington,</i> 805, 1–35."));


    }


}
