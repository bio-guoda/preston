package bio.guoda.preston.zenodo;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.DerefState;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.cmd.JavaScriptAndPythonFriendlyURLEncodingUtil;
import bio.guoda.preston.store.Dereferencer;
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
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static bio.guoda.preston.cmd.ZenodoMetaUtil.RESOURCE_TYPE_PHOTO;

public class ZenodoUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ZenodoUtils.class);

    private final static String APPLICATION_JSON = ContentType.APPLICATION_JSON.getMimeType();

    private static ZenodoContext updateContext(ZenodoContext ctx, InputStream is) throws IOException {
        ZenodoContext copy = new ZenodoContext(ctx);
        JsonNode response = new ObjectMapper().readTree(is);
        copy.setMetadata(response);
        JsonNode deposit = response.at("/id");
        if (deposit != null) {
            copy.setDepositId(deposit.asLong());
        }

        JsonNode bucket = response.at("/links/bucket");
        if (bucket != null) {
            Matcher matcher = UUIDUtil.ENDING_WITH_UUID_PATTERN.matcher(bucket.asText());
            if (matcher.matches()) {
                copy.setBucketId(UUID.fromString(matcher.group("uuid")));
            }
        }
        return copy;
    }

    static ObjectMapper getObjectMapper() {
        JsonFactory jf = JsonFactory.builder()
                .enable(JsonWriteFeature.ESCAPE_NON_ASCII)
                .build();
        return new ObjectMapper(jf);
    }

    public static void delete(ZenodoContext ctx) throws IOException {
        String deleteRequestURI = ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId();
        try (InputStream inputStream = delete(deleteRequestURI, ctx)) {
            updateContext(ctx, inputStream);
        }
    }

    public static InputStream delete(String deleteRequestURI, ZenodoContext ctx) throws IOException {
        IRI dataURI = RefNodeFactory.toIRI(deleteRequestURI);
        return delete(dataURI, ctx);
    }

    public static InputStream delete(IRI dataURI, ZenodoContext ctx) throws IOException {
        HttpDelete request = new HttpDelete(dataURI.getIRIString());
        ResourcesHTTP.appendAuthBearerIfAvailable(request, ctx.getAccessToken());

        return ResourcesHTTP.asInputStream(
                dataURI,
                request,
                ignoreProgress(),
                ignoreNone()
        );
    }

    public static ZenodoContext update(ZenodoContext ctx, String metadata) throws IOException {
        String requestURI = ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId();
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        HttpPut request = new HttpPut(URI.create(requestURI));
        ResourcesHTTP.appendAuthBearerIfAvailable(request, ctx.getAccessToken());

        BasicHttpEntity entity = new BasicHttpEntity();
        byte[] bytes = metadata.getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        entity.setContent(is);
        entity.setContentLength(bytes.length);
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

    public static ZenodoContext get(ZenodoContext ctx) throws IOException {
        String requestURI = ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId();
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        HttpGet request = new HttpGet(URI.create(requestURI));
        ResourcesHTTP.appendAuthBearerIfAvailable(request, ctx.getAccessToken());

        try (InputStream is = ResourcesHTTP.asInputStream(
                dataURI,
                request,
                ignoreProgress(),
                ignoreNone()
        )) {
            return updateContext(ctx, is);
        }
    }

    public static ZenodoContext createNewVersion(ZenodoContext ctx) throws IOException {
        String requestURI = ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId() + "/actions/newversion";
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        HttpPost request = new HttpPost(URI.create(dataURI.getIRIString()));
        ResourcesHTTP.appendAuthBearerIfAvailable(request, ctx.getAccessToken());
        try (InputStream is = ResourcesHTTP.asInputStream(
                dataURI,
                request,
                ignoreProgress(),
                ignoreNone()
        )) {
            return updateContext(ctx, is);
        }
    }

    public static ZenodoContext editExistingVersion(ZenodoContext ctx) throws IOException {
        String requestURI = ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId() + "/actions/edit";
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        HttpPost request = new HttpPost(URI.create(dataURI.getIRIString()));
        ResourcesHTTP.appendAuthBearerIfAvailable(request, ctx.getAccessToken());
        try (InputStream is = ResourcesHTTP.asInputStream(
                dataURI,
                request,
                ignoreProgress(),
                ignoreNone()
        )) {
            return updateContext(ctx, is);
        }
    }

    static DerefProgressListener ignoreProgress() {
        return new DerefProgressListener() {
            @Override
            public void onProgress(IRI dataURI, DerefState derefState, long read, long total) {

            }
        };
    }

    static Predicate<Integer> ignoreNone() {
        return new Predicate<Integer>() {
            @Override
            public boolean test(Integer integer) {
                return false;
            }
        };
    }

    public static ZenodoContext createEmptyDeposit(ZenodoContext ctx) throws IOException {
        String requestURI = ctx.getEndpoint() + "/api/deposit/depositions";
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        HttpPost request = new HttpPost(URI.create(dataURI.getIRIString()));
        ResourcesHTTP.appendAuthBearerIfAvailable(request, ctx.getAccessToken());
        request.setHeader("Accept", "*/*");
        BasicHttpEntity entity = new BasicHttpEntity();

        String input = "{}";
        entity.setContent(IOUtils.toInputStream(input, StandardCharsets.UTF_8));
        entity.setContentLength(input.length());
        entity.setContentType(APPLICATION_JSON);
        request.setEntity(entity);
        try (InputStream is = ResourcesHTTP.asInputStream(
                dataURI,
                request,
                ignoreProgress(),
                ignoreNone()
        )) {
            return updateContext(ctx, is);
        }
    }

    public static ZenodoContext publish(ZenodoContext ctx) throws IOException {
        String requestURI = ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId() + "/actions/publish";
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        HttpPost request = new HttpPost(URI.create(dataURI.getIRIString()));
        ResourcesHTTP.appendAuthBearerIfAvailable(request, ctx.getAccessToken());
        try (InputStream inputStream = ResourcesHTTP.asInputStream(
                dataURI,
                request,
                ignoreProgress(),
                ignoreNone()
        )) {
            return updateContext(ctx, inputStream);
        } catch (HttpResponseException ex) {
            if (HttpStatus.SC_BAD_REQUEST == ex.getStatusCode()) {
                String metadata = ctx.getMetadata() == null ? "" : ": [" + ctx.getMetadata().toPrettyString() + "]";
                throw new IOException("Zenodo deposition publication request was rejected. Please review possibly malformed or incomplete record metadata" + metadata + ".", ex);
            } else {
                throw ex;
            }
        }
    }

    public static ZenodoContext upload(ZenodoContext ctx, String filename, HttpEntity entity) throws IOException {
        String requestURI = ctx.getEndpoint() + "/api/files/" + ctx.getBucketId() + "/" + JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode(filename);
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        HttpPut request = new HttpPut(requestURI);
        request.setEntity(entity);
        ResourcesHTTP.appendAuthBearerIfAvailable(request, ctx.getAccessToken());


        try (InputStream is = ResourcesHTTP.asInputStream(
                dataURI,
                request,
                ignoreProgress(),
                ignoreNone()
        )) {
            return updateContext(ctx, is);
        }
    }

    public static Collection<Pair<Long, String>> findRecordsByAlternateIds(ZenodoConfig ctx, List<String> ids, String type, Dereferencer<InputStream> dereferencer) throws IOException {
        Collection<Pair<Long, String>> foundIds = new TreeSet<>();
        findExistingRecords(ctx, ids, foundIds, type, dereferencer);
        return foundIds;
    }


    public static IRI getQueryForExistingDepositions(ZenodoConfig ctx, List<String> contentIds, String method, String type) {
        String prefix = communitiesPrefix(ctx);
        String query = prefix + "q=" + getQueryForIds(contentIds);
        query = appendTypeClause(type, query, method);
        return getQuery(ctx.getEndpoint(), query, method);
    }

    private static String appendTypeClause(String type, String query, String method) {
        if ( StringUtils.isNotBlank(type)) {
            if (StringUtils.startsWith(method, "/search")) {
                query = query + "&f=resource_type%3A" + JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode(type);
            } else {
                // see https://github.com/zenodo/zenodo/issues/2545
                if (RESOURCE_TYPE_PHOTO.equals(type) || "image-photo".equals(type)) {
                    query = query + "&type=image&subtype=photo";
                } else {
                    query = query + "&type=" + JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode(type);
                }
            }
        }
        return query;
    }

    public static IRI getSearchPageForExistingRecords(ZenodoConfig ctx, List<String> contentIds, String type) {
        String prefix = communitiesPrefix(ctx);
        String query = prefix + "q=" + getQueryForIds(contentIds);
        query = appendTypeClause(type, query, "/search");
        return getQuery(ctx.getEndpoint(), query, "/search");
    }

    private static void findExistingRecords(ZenodoConfig ctx, List<String> ids, Collection<Pair<Long, String>> foundIds, String type, Dereferencer<InputStream> dereferencer) throws IOException {
        IRI query = getQueryForExistingRecords(ctx, ids, type);
        executeQueryAndCollectIds(foundIds, query, dereferencer);
    }

    public static IRI getQueryForExistingRecords(ZenodoConfig ctx, List<String> ids, String type) {
        String prefix = communitiesPrefix(ctx);
        String queryPath = prefix + "all_versions=false&q=" + getQueryForIds(ids);
        String method = "/api/records";
        return getQuery(ctx.getEndpoint(), appendTypeClause(type, queryPath, method), method);
    }

    private static String communitiesPrefix(ZenodoConfig ctx) {
        return ctx.getCommunities().size() == 0
                    ? ""
                    : "communities=" + JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode(StringUtils.join(ctx.getCommunities(), ",")) + "&";
    }

    private static String getQueryForIds(List<String> ids) {
        return ids.stream()
                .map(JavaScriptAndPythonFriendlyURLEncodingUtil::urlEncode)
                .map(id -> "alternate.identifier:%22" + id + "%22")
                .collect(Collectors.joining("%20AND%20"));
    }

    private static void executeQueryAndCollectIds(Collection<Pair<Long, String>> foundIds, IRI query, Dereferencer<InputStream> dereferencer) throws IOException {

        LOG.info("executing query [" + query + "]...");
        try (InputStream is = dereferencer.get(query)) {
            JsonNode jsonNode = getObjectMapper().readTree(is);
            LOG.info("got query result [" + jsonNode.toPrettyString() + "]");
            JsonNode hits = jsonNode.has("hits")
                    && jsonNode.get("hits").has("hits") ? jsonNode.get("hits").get("hits") : jsonNode;
            for (JsonNode hit : hits) {
                if (hit.has("id")
                        && hit.get("id").isIntegralNumber()
                        && hit.has("state")) {
                    foundIds.add(Pair.of(hit.get("id").asLong(), hit.get("state").asText()));
                }
            }
        } finally {
            LOG.info("executing query [" + query + "] done.");

        }
    }

    private static IRI getQuery(String apiEndpoint, String filter, String method) {
        return RefNodeFactory.toIRI((apiEndpoint + method) + "?" + filter);
    }

    public static List<IRI> getFileEndpoints(ZenodoContext ctx) {
        JsonNode metadata = ctx.getMetadata();
        JsonNode files = metadata == null ? new ObjectMapper().createArrayNode() : metadata.at("/files");
        List<String> ids = new ArrayList<>();
        for (JsonNode file : files) {
            JsonNode at = file.at("/id");
            if (!at.isMissingNode()) {
                ids.add(at.asText());
            }
        }
        return ids
                .stream()
                .map(id -> ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId() + "/files/" + id)
                .map(RefNodeFactory::toIRI).collect(Collectors.toList());
    }
}
