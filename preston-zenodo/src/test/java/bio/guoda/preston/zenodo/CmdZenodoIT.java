package bio.guoda.preston.zenodo;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.cmd.ZenodoMetaUtil;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.startsWith;


public class CmdZenodoIT {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void createOrUpdateZenodo() throws URISyntaxException, IOException {
        CmdZenodo cmdZenodo = new CmdZenodo();
        String resourceURI = "taxodros-data/6e/f3/6ef3b8e326cd52972da1c00de60dc222";

        File dataDir = folder.newFolder("zenodo-test");
        cmdZenodo.setLocalDataDir(dataDir.getAbsolutePath());
        cmdZenodo.setApiEndpoint("https://sandbox.zenodo.org");

        System.setProperty("ZENODO_TOKEN", ZenodoTestUtil.getAccessToken());

        cmdZenodo.setInputStream(getClass().getResourceAsStream(resourceURI));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdZenodo.setOutputStream(outputStream);
        cmdZenodo.setCacheEnabled(false);

        AtomicInteger counter = new AtomicInteger();
        AtomicReference<IRI> requested = new AtomicReference<>(null);
        cmdZenodo.run(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                requested.set(uri);
                counter.incrementAndGet();
                return getClass().getResourceAsStream("taxodros-data/7e/5a/7e5ae7ff14d66bff5224b21c80cdb87d");
            }
        });
        String log = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        String[] split = StringUtils.split(log, '\n');
        assertThat(split.length, greaterThan(0));
        assertThat(split[split.length-1], startsWith("<https://sandbox.zenodo.org/records/"));
        assertThat(split[split.length-1], containsString("> <http://www.w3.org/ns/prov#wasDerivedFrom> <line:hash://md5/7e5ae7ff14d66bff5224b21c80cdb87d!/L1> <urn:uuid:"));
    }


    @Test
    public void createDepositAndUpdateToRestrict() throws URISyntaxException, IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CmdZenodo cmdZenodo = createCmd(outputStream);

        // first make sure a deposit exists
        cmdZenodo.run();
        String log = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        String[] split = StringUtils.split(log, '\n');
        assertThat(split.length, greaterThan(0));

        assertThat(log, containsString("https://sandbox.zenodo.org/records/"));

        Pattern compile = Pattern.compile(".*https://sandbox.zenodo.org/records/(?<depositId>[0-9]+).*");
        Matcher matcher = compile.matcher(StringUtils.replace(log, "\n", " "));
        assertThat(matcher.matches(), Is.is(true));


        String depositId = matcher.group("depositId");
        JsonNode depositMetadata = requestDepositMetadata(depositId);

        assertThat(depositMetadata.at("/metadata").isMissingNode(), Is.is(false));

        outputStream = new ByteArrayOutputStream();
        cmdZenodo = createCmd(outputStream);

        // then update metadata to be open
        cmdZenodo.setUpdateMetadataOnly(true);
        cmdZenodo.setPublishRestrictedOnly(false);
        cmdZenodo.run();

        depositMetadata = requestDepositMetadata(depositId);

        assertThat(depositMetadata.at("/metadata/" + ZenodoMetaUtil.ACCESS_RIGHT).asText(), Is.is("open"));

        outputStream = new ByteArrayOutputStream();
        cmdZenodo = createCmd(outputStream);

        // then update metadata to be restricted
        cmdZenodo.setUpdateMetadataOnly(true);
        cmdZenodo.setPublishRestrictedOnly(true);
        cmdZenodo.run();

        depositMetadata = requestDepositMetadata(depositId);

        assertThat(depositMetadata.at("/metadata/" + ZenodoMetaUtil.ACCESS_RIGHT).asText(), Is.is("restricted"));


    }

    private CmdZenodo createCmd(ByteArrayOutputStream outputStream) throws IOException {
        CmdZenodo cmdZenodo = new CmdZenodo();
        String resourceURI = "batlit-data/31/31/3131a4dd8ed099a31e2f2032c0248ba7";

        URL resource = getClass().getResource(resourceURI);
        URI remote = new File(resource.getFile()).getParentFile().getParentFile().getParentFile().toURI();


        File dataDir = folder.newFolder();
        cmdZenodo.setLocalDataDir(dataDir.getAbsolutePath());
        cmdZenodo.setRemotes(Arrays.asList(remote));
        cmdZenodo.setApiEndpoint("https://sandbox.zenodo.org");

        System.setProperty("ZENODO_TOKEN", ZenodoTestUtil.getAccessToken());

        cmdZenodo.setInputStream(getClass().getResourceAsStream(resourceURI));
        cmdZenodo.setOutputStream(outputStream);
        cmdZenodo.setCacheEnabled(false);
        return cmdZenodo;
    }

    private JsonNode requestDepositMetadata(String depositId) throws IOException {
        InputStream inputStream = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI("https://sandbox.zenodo.org/api/records/" + depositId));
        return new ObjectMapper().readTree(inputStream);
    }


}