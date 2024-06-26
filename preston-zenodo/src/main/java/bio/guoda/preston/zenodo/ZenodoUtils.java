package bio.guoda.preston.zenodo;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.DerefState;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.cmd.JavaScriptAndPythonFriendlyURLEncodingUtil;
import bio.guoda.preston.util.UUIDUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentType;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public class ZenodoUtils {


    private final static String APPLICATION_JSON = ContentType.APPLICATION_JSON.getMimeType();

    private static ZenodoContext updateContext(ZenodoContext ctx, InputStream is) throws IOException {
        ZenodoContext copy = new ZenodoContext(ctx.getAccessToken(), ctx.getEndpoint());
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
        String deleteRequestURI = ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId() + "?access_token=" + ctx.getAccessToken();
        try (InputStream inputStream = ResourcesHTTP.asInputStream(
                RefNodeFactory.toIRI(deleteRequestURI),
                new HttpDelete(URI.create(deleteRequestURI)),
                ignoreProgress(),
                ignoreNone()
        )) {
            updateContext(ctx, inputStream);
        }
    }

    public static ZenodoContext update(ZenodoContext ctx, String metadata) throws IOException {
        String requestURI = ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId() + "?access_token=" + ctx.getAccessToken();
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

    public static ZenodoContext get(ZenodoContext ctx) throws IOException {
        String requestURI = ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId() + "?access_token=" + ctx.getAccessToken();
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        HttpGet request = new HttpGet(URI.create(requestURI));

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
        String requestURI = ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId() + "/actions/newversion?access_token=" + ctx.getAccessToken();
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        try (InputStream is = ResourcesHTTP.asInputStream(
                dataURI,
                new HttpPost(URI.create(dataURI.getIRIString())),
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

    public static ZenodoContext create(ZenodoContext ctx, JsonNode jsonNode) throws IOException {
        String requestURI = ctx.getEndpoint() + "/api/deposit/depositions?access_token=" + ctx.getAccessToken();
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        JsonNode payload = jsonNode == null ? getObjectMapper().createObjectNode() : jsonNode;
        HttpPost request = new HttpPost(URI.create(dataURI.getIRIString()));
        request.setHeader("Accept", "*/*");
        BasicHttpEntity entity = new BasicHttpEntity();

        String input = getObjectMapper().writer().writeValueAsString(payload);
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
        String requestURI = ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId() + "/actions/publish?access_token=" + ctx.getAccessToken();
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        try (InputStream inputStream = ResourcesHTTP.asInputStream(
                dataURI,
                new HttpPost(URI.create(dataURI.getIRIString())),
                ignoreProgress(),
                ignoreNone()
        )) {
            return updateContext(ctx, inputStream);
        }
    }

    public static ZenodoContext upload(ZenodoContext ctx, String filename, HttpEntity entity) throws IOException {
        String requestURI = ctx.getEndpoint() + "/api/files/" + ctx.getBucketId() + "/" + JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode(filename) + "?access_token=" + ctx.getAccessToken();
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        HttpPut request = new HttpPut(requestURI);
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

    public static Collection<Pair<Long, String>> findByAlternateIds(ZenodoConfig ctx, List<String> contentIds) throws IOException {
        Collection<Pair<Long, String>> foundIds = new TreeSet<>();
        appendIds(foundIds, ctx.getEndpoint(), "communities=taxodros&all_versions=false&q=" + getQueryForIds(contentIds) + "&access_token=" + ctx.getAccessToken(), "/api/records" );
        appendIds(foundIds, ctx.getEndpoint(), "q=" + getQueryForIds(contentIds) + "&access_token=" + ctx.getAccessToken(), "/api/deposit/depositions");
        return foundIds;
    }

    private static String getQueryForIds(List<String> ids) {
        return ids.stream()
                .map(JavaScriptAndPythonFriendlyURLEncodingUtil::urlEncode)
                .map(id -> "alternate.identifier:%22" + id + "%22")
                .collect(Collectors.joining("%20AND%20"));
    }

    private static void appendIds(Collection<Pair<Long, String>> foundIds, String apiEndpoint, String filter, String method) throws IOException {
        IRI query = RefNodeFactory.toIRI((apiEndpoint + method) + "?" + filter);
        try (InputStream is = ResourcesHTTP.asInputStream(query)) {
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
}
