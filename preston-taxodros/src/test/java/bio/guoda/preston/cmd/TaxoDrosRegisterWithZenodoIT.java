package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.DerefState;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonpCharacterEscapes;
import com.fasterxml.jackson.core.io.CharacterEscapes;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.core.json.UTF8JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.util.EncodingUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;

public class TaxoDrosRegisterWithZenodoIT {

    public static final String CONTENT_ID_PDF = "hash://md5/639988a4074ded5208a575b760a5dc5e";
    public static final String TAXODROS_ID = "urn:lsid:taxodros.uzh.ch:id:abd%20el-halim%20et%20al.,%202005";
    public static final String ZENODO_API_ENDPOINT = "https://sandbox.zenodo.org";

    public Long depositId;

    @Before
    public void createDeposit() throws IOException {
        InputStream request = getClass().getResourceAsStream("zenodo-metadata.json");
        ObjectMapper objectMapper = getObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(IOUtils.toString(request, StandardCharsets.UTF_8));
        InputStream is = createDeposit(getAccessToken(), jsonNode);
        JsonNode response = new ObjectMapper().readTree(is);
        if (response.has("id") && response.get("id").isIntegralNumber()) {
            depositId = response.get("id").asLong();
        } else {
            fail("failed to create zenodo deposit");
        }
    }

    private static ObjectMapper getObjectMapper() {
        JsonFactory jf = JsonFactory.builder()
                .enable(JsonWriteFeature.ESCAPE_NON_ASCII)
                .build();
        return new ObjectMapper(jf);
    }

    @After
    public void deleteDeposit() throws IOException {
        deleteDeposit(getAccessToken(), depositId);
    }


    @Test
    public void findDepositByHash() throws IOException {
        assertOneRecordWithMatchingId(Arrays.asList(CONTENT_ID_PDF));
    }

    @Test
    public void findDepositByTaxoDrosId() throws IOException {
        assertOneRecordWithMatchingId(Arrays.asList(TAXODROS_ID));
    }

    @Test
    public void findDepositByBothContentIdAndTaxoDrosId() throws IOException {
        assertOneRecordWithMatchingId(Arrays.asList(CONTENT_ID_PDF, TAXODROS_ID));
    }

    private void assertOneRecordWithMatchingId(List<String> contentId) throws IOException {

        Collection<Pair<Long, String>> ids = findByAlternateIds(contentId, getAccessToken());
        assertThat(ids, not(nullValue()));
        List<Long> filteredIds = ids
                .stream()
                .filter(x -> depositId.equals(x.getKey()))
                .map(Pair::getKey)
                .collect(Collectors.toList());
        assertThat(filteredIds.size(), is(1));
        assertThat(filteredIds.get(0), is(depositId));

    }


    @Test
    public void uploadData() throws IOException {
    }

    @Test
    public void uploadMetadata() throws IOException {
    }

    @Test
    public void createOrUpdateDeposit() throws IOException {
        String accessToken = getAccessToken();

        Collection<Pair<Long, String>> matchingRecords
                = findByAlternateIds(Arrays.asList(CONTENT_ID_PDF), getAccessToken());
        List<Pair<Long, String>> unsubmitted = matchingRecords
                .stream()
                .filter(hit -> StringUtils.equals(hit.getValue(), "unsubmitted"))
                .collect(Collectors.toList());

        for (Pair<Long, String> unsubmittedDeposit : unsubmitted) {
            Long depositId = unsubmittedDeposit.getKey();
            deleteDeposit(accessToken, depositId);

        }

        List<Pair<Long, String>> submitted = matchingRecords
                .stream()
                .filter(hit -> StringUtils.equals(hit.getValue(), "done"))
                .collect(Collectors.toList());


        assertThat(submitted.size(), is(1));

        Long depositId = submitted.stream().findFirst().get().getKey();
        InputStream is = createDepositVersion(accessToken, depositId);
        JsonNode response = getObjectMapper().readTree(is);

        assertThat(response.get("id"), is(notNullValue()));
        assertThat(response.get("id").asLong(), is(greaterThan(matchingRecords.stream().findFirst().get().getKey())));

    }

    private static InputStream createDepositVersion(String accessToken, Long depositId) throws IOException {
        String requestURI = ZENODO_API_ENDPOINT + "/api/deposit/depositions/" + depositId + "/actions/newversion?access_token=" + accessToken;
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        return ResourcesHTTP.asInputStream(
                dataURI,
                new HttpPost(URI.create(dataURI.getIRIString())),
                ignoreProgress(),
                ignoreNone()
        );
    }

    private static InputStream createDeposit(String accessToken) throws IOException {
        return createDeposit(accessToken, getObjectMapper().createObjectNode());
    }


    private static InputStream createDeposit(String accessToken, JsonNode metadata) throws IOException {
        String requestURI = ZENODO_API_ENDPOINT + "/api/deposit/depositions?access_token=" + accessToken;
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        JsonNode payload = metadata == null ? getObjectMapper().createObjectNode() : metadata;
        HttpPost request = new HttpPost(URI.create(dataURI.getIRIString()));
        request.setHeader("Accept", "*/*");
        BasicHttpEntity entity = new BasicHttpEntity();

        String input = getObjectMapper().writer().writeValueAsString(payload);
        entity.setContent(IOUtils.toInputStream(input, StandardCharsets.UTF_8));
        entity.setContentLength(input.length());
        entity.setContentType("application/json");
        request.setEntity(entity);
        return ResourcesHTTP.asInputStream(
                dataURI,
                request,
                ignoreProgress(),
                ignoreNone()
        );
    }

    private static InputStream updateDeposit(String accessToken, JsonNode metadata, Long depositId) throws IOException {
        String requestURI = ZENODO_API_ENDPOINT + "/api/deposit/depositions/" + depositId + "?access_token=" + accessToken;
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        HttpPost request = new HttpPost(URI.create(dataURI.getIRIString()));
        BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContent(IOUtils.toInputStream(metadata.toString(), StandardCharsets.UTF_8));
        request.setEntity(entity);
        return ResourcesHTTP.asInputStream(
                dataURI,
                request,
                ignoreProgress(),
                ignoreNone()
        );
    }

    private static void deleteDeposit(String accessToken, Long depositId) throws IOException {
        String deleteRequestURI = ZENODO_API_ENDPOINT + "/api/deposit/depositions/" + depositId + "?access_token=" + accessToken;
        ResourcesHTTP.asInputStream(
                RefNodeFactory.toIRI(deleteRequestURI),
                new HttpDelete(URI.create(deleteRequestURI)),
                ignoreProgress(),
                ignoreNone()
        );
    }

    private static DerefProgressListener ignoreProgress() {
        return new DerefProgressListener() {
            @Override
            public void onProgress(IRI dataURI, DerefState derefState, long read, long total) {

            }
        };
    }

    private static Predicate<Integer> ignoreNone() {
        return new Predicate<Integer>() {
            @Override
            public boolean test(Integer integer) {
                return false;
            }
        };
    }

    private String getAccessToken() throws IOException {
        return IOUtils.toString(getClass().getResourceAsStream("zenodo-token.hidden"), StandardCharsets.UTF_8);
    }

    private Collection<Pair<Long, String>> findByAlternateIds(List<String> contentIds, String accessToken) throws IOException {
        Collection<Pair<Long, String>> foundIds = new TreeSet<>();
        appendIds(foundIds, ZENODO_API_ENDPOINT, "communities=taxodros&all_versions=true&q=" + getQueryForIds(contentIds), "/api/records");
        appendIds(foundIds, ZENODO_API_ENDPOINT, "q=" + getQueryForIds(contentIds) + "&access_token=" + accessToken, "/api/deposit/depositions");

        return foundIds;
    }

    private String getQueryForIds(List<String> ids) {
        return ids.stream()
                .map(URLEncodingUtil::urlEncode)
                .map(id -> "alternate.identifier:%22" + id + "%22")
                .collect(Collectors.joining("%20AND%20"));
    }

    private void appendIds(Collection<Pair<Long, String>> foundIds, String apiEndpoint, String filter, String method) throws IOException {
        InputStream is = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI((apiEndpoint
                + method) + "?" + filter));

        JsonNode jsonNode = getObjectMapper().readTree(is);

        JsonNode hits = jsonNode.has("hits")
                && jsonNode.get("hits").has("hits") ? jsonNode.get("hits").get("hits") : jsonNode;

        for (JsonNode hit : hits) {
            if (hit.has("id")
                    && hit.get("id").isIntegralNumber()
                    && hit.has("state")) {
                foundIds.add(Pair.of(hit.get("id").asLong(), hit.get("state").asText()));
            }
        }
    }


}
