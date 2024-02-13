package bio.guoda.preston.zenodo;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.DerefState;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.cmd.URLEncodingUtil;
import bio.guoda.preston.util.UUIDUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Matcher;

public class ZenodoUtils {


    private final static String APPLICATION_JSON = ContentType.APPLICATION_JSON.getMimeType();

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

    static ObjectMapper getObjectMapper() {
        JsonFactory jf = JsonFactory.builder()
                .enable(JsonWriteFeature.ESCAPE_NON_ASCII)
                .build();
        return new ObjectMapper(jf);
    }

    public static void delete(ZenodoContext ctx) throws IOException {
        String deleteRequestURI = ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId() + "?access_token=" + ctx.getAccessToken();
        ResourcesHTTP.asInputStream(
                RefNodeFactory.toIRI(deleteRequestURI),
                new HttpDelete(URI.create(deleteRequestURI)),
                ignoreProgress(),
                ignoreNone()
        );
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

    public static ZenodoContext createNewVersion(ZenodoContext ctx) throws IOException {
        String requestURI = ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId() + "/actions/newversion?access_token=" + ctx.getAccessToken();
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
        InputStream is = ResourcesHTTP.asInputStream(
                dataURI,
                request,
                ignoreProgress(),
                ignoreNone()
        );
        return updateContext(ctx, is);
    }

    public static void publish(ZenodoContext ctx) throws IOException {
        String requestURI = ctx.getEndpoint() + "/api/deposit/depositions/" + ctx.getDepositId() + "/actions/publish?access_token=" + ctx.getAccessToken();
        IRI dataURI = RefNodeFactory.toIRI(requestURI);
        ResourcesHTTP.asInputStream(
                dataURI,
                new HttpPost(URI.create(dataURI.getIRIString())),
                ignoreProgress(),
                ignoreNone()
        );
    }

    public static void upload(ZenodoContext ctx, String filename, InputStream is) throws IOException {
        String requestURI = ctx.getEndpoint() + "/api/files/" + ctx.getBucketId() + "/" + URLEncodingUtil.urlEncode(filename) + "?access_token=" + ctx.getAccessToken();
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
}
