package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.DerefState;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.util.UUIDUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
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
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;

public class TaxoDrosRegisterWithZenodoIT {

    public static final String CONTENT_ID_PDF = "hash://md5/639988a4074ded5208a575b760a5dc5e";
    public static final String TAXODROS_ID = "urn:lsid:taxodros.uzh.ch:id:abd%20el-halim%20et%20al.,%202005";
    public static final String ZENODO_API_ENDPOINT = "https://sandbox.zenodo.org";
    public static final String APPLICATION_JSON = ContentType.APPLICATION_JSON.getMimeType();

    private ZenodoContext ctx = null;

    @Before
    public void create() throws IOException {
        InputStream request = getInputStream();
        ObjectMapper objectMapper = getObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(IOUtils.toString(request, StandardCharsets.UTF_8));
        ctx = new ZenodoContext(getAccessToken());
        ctx = create(ctx, jsonNode);

        assertNotNull(ctx);
        assertNotNull(ctx.getBucketId());
        assertNotNull(ctx.getDepositId());

    }

    @After
    public void delete() throws IOException {
        try {
            delete(this.ctx);
            cleanupPreExisting();
        } catch (IOException ex) {
            // ignore
        }
    }

    private void cleanupPreExisting() throws IOException {
        Collection<Pair<Long, String>> byAlternateIds = findByAlternateIds(ctx, Arrays.asList(CONTENT_ID_PDF, TAXODROS_ID));
        byAlternateIds
                .stream()
                .filter(d -> StringUtils.equals(d.getValue(), "unsubmitted"))
                .map(Pair::getKey)
                .forEach(depositId -> {
                    ZenodoContext ctx = new ZenodoContext(this.ctx.getAccessToken());
                    ctx.setDepositId(depositId);
                    try {
                        delete(ctx);
                    } catch (IOException e) {
                        // ignore
                    }
                });
    }


    private ZenodoContext create(ZenodoContext ctx, JsonNode jsonNode) throws IOException {
        InputStream is = create(ctx.getAccessToken(), jsonNode);
        return updateContext(ctx, is);
    }

    private static ZenodoContext updateContext(ZenodoContext ctx, InputStream is) throws IOException {
        JsonNode response = new ObjectMapper().readTree(is);
        JsonNode deposit = response.at("/id");
        if (deposit != null) {
            ctx.setDepositId(deposit.asLong());
        }

        JsonNode bucket = response.at("/links/bucket");
        if (bucket != null) {
            Matcher matcher = UUIDUtil.ENDING_WITH_UUID_PATTERN.matcher(bucket.asText());
            if (matcher.matches()) {
                ctx.setBucketId(UUID.fromString(matcher.group("uuid")));
            }
        }

        return ctx;
    }

    private static ObjectMapper getObjectMapper() {
        JsonFactory jf = JsonFactory.builder()
                .enable(JsonWriteFeature.ESCAPE_NON_ASCII)
                .build();
        return new ObjectMapper(jf);
    }


    private void delete(ZenodoContext ctx) throws IOException {
        String deleteRequestURI = ZENODO_API_ENDPOINT + "/api/deposit/depositions/" + ctx.getDepositId() + "?access_token=" + ctx.getAccessToken();
        ResourcesHTTP.asInputStream(
                RefNodeFactory.toIRI(deleteRequestURI),
                new HttpDelete(URI.create(deleteRequestURI)),
                ignoreProgress(),
                ignoreNone()
        );
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

        Collection<Pair<Long, String>> ids = findByAlternateIds(ctx, contentId);
        assertThat(ids, not(nullValue()));
        List<Long> filteredIds = ids
                .stream()
                .filter(x -> ctx.getDepositId().equals(x.getKey()))
                .map(Pair::getKey)
                .collect(Collectors.toList());
        assertThat(filteredIds.size(), is(1));
        assertThat(filteredIds.get(0), is(ctx.getDepositId()));

    }


    @Test
    public void uploadData() throws IOException {
        InputStream resourceAsStream = getInputStream();
        assertNotNull(resourceAsStream);
        upload(this.ctx, "some spacey name.json", resourceAsStream);
    }

    private InputStream getInputStream() {
        return getClass().getResourceAsStream("zenodo-metadata.json");
    }

    @Test
    public void updateMetadata() throws IOException {
        InputStream inputStream = getInputStream();
        JsonNode payload = getObjectMapper().readTree(inputStream);
        String input = getObjectMapper().writer().writeValueAsString(payload);
        update(this.ctx, input);
    }

    public static ZenodoContext update(ZenodoContext ctx, String metadata) throws IOException {
        String requestURI = ZENODO_API_ENDPOINT + "/api/deposit/depositions/" + ctx.getDepositId() + "?access_token=" + ctx.getAccessToken();
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        HttpPut request = new HttpPut(URI.create(requestURI));

        BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContent(IOUtils.toInputStream(metadata, StandardCharsets.UTF_8));
        entity.setContentLength(metadata.length());
        entity.setContentType(APPLICATION_JSON);
        request.setEntity(entity);
        InputStream inputStream1 = ResourcesHTTP.asInputStream(
                dataURI,
                request,
                ignoreProgress(),
                ignoreNone()
        );
        return updateContext(ctx, inputStream1);
    }

    @Test
    public void createNewVersion() throws IOException {

        InputStream resourceAsStream = getInputStream();

        assertNotNull(resourceAsStream);

        upload(this.ctx, "some spacey name.json", resourceAsStream);

        publish(this.ctx);
        Long depositIdPrevious = ctx.getDepositId();
        ctx = createNewVersion(this.ctx);
        assertThat(ctx.getDepositId(), is(notNullValue()));
        assertThat(ctx.getDepositId(), is(greaterThan(depositIdPrevious)));

    }

    private static ZenodoContext createNewVersion(ZenodoContext ctx) throws IOException {
        String requestURI = ZENODO_API_ENDPOINT + "/api/deposit/depositions/" + ctx.getDepositId() + "/actions/newversion?access_token=" + ctx.getAccessToken();
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        InputStream is = ResourcesHTTP.asInputStream(
                dataURI,
                new HttpPost(URI.create(dataURI.getIRIString())),
                ignoreProgress(),
                ignoreNone()
        );
        JsonNode response = new ObjectMapper().readTree(is);
        JsonNode deposit = response.at("/id");
        if (deposit != null) {
            ctx.setDepositId(deposit.asLong());
        }

        JsonNode bucket = response.at("/links/bucket");
        if (bucket != null) {
            Matcher matcher = UUIDUtil.ENDING_WITH_UUID_PATTERN.matcher(bucket.asText());
            if (matcher.matches()) {
                ctx.setBucketId(UUID.fromString(matcher.group("uuid")));
            }
        }

        return ctx;
    }

    private void publish(ZenodoContext ctx) throws IOException {
        String requestURI = ZENODO_API_ENDPOINT + "/api/deposit/depositions/" + ctx.getDepositId() + "/actions/publish?access_token=" + ctx.getAccessToken();
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        ResourcesHTTP.asInputStream(
                dataURI,
                new HttpPost(URI.create(dataURI.getIRIString())),
                ignoreProgress(),
                ignoreNone()
        );
    }

    private void upload(ZenodoContext ctx, String filename, InputStream is) throws IOException {
        String requestURI = ZENODO_API_ENDPOINT + "/api/files/" + ctx.getBucketId() + "/" + URLEncodingUtil.urlEncode(filename) + "?access_token=" + ctx.getAccessToken();
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        HttpPut request = new HttpPut(URI.create(dataURI.getIRIString()));
        HttpEntity entity = new InputStreamEntity(is);
        request.setEntity(entity);
        ResourcesHTTP.asInputStream(
                dataURI,
                request,
                ignoreProgress(),
                ignoreNone()
        );
    }


    private static InputStream create(String accessToken, JsonNode metadata) throws IOException {
        String requestURI = ZENODO_API_ENDPOINT + "/api/deposit/depositions?access_token=" + accessToken;
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        JsonNode payload = metadata == null ? getObjectMapper().createObjectNode() : metadata;
        HttpPost request = new HttpPost(URI.create(dataURI.getIRIString()));
        request.setHeader("Accept", "*/*");
        BasicHttpEntity entity = new BasicHttpEntity();

        String input = getObjectMapper().writer().writeValueAsString(payload);
        entity.setContent(IOUtils.toInputStream(input, StandardCharsets.UTF_8));
        entity.setContentLength(input.length());
        entity.setContentType(APPLICATION_JSON);
        request.setEntity(entity);
        return ResourcesHTTP.asInputStream(
                dataURI,
                request,
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

    private Collection<Pair<Long, String>> findByAlternateIds(ZenodoContext ctx, List<String> contentIds) throws IOException {
        Collection<Pair<Long, String>> foundIds = new TreeSet<>();
        appendIds(foundIds, ZENODO_API_ENDPOINT, "communities=taxodros&all_versions=true&q=" + getQueryForIds(contentIds), "/api/records");
        appendIds(foundIds, ZENODO_API_ENDPOINT, "q=" + getQueryForIds(contentIds) + "&access_token=" + ctx.getAccessToken(), "/api/deposit/depositions");
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
