package bio.guoda.preston.zenodo;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.cmd.ZenodoMetaUtil;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.VersionUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;


public class CmdZenodoIT {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void createOrUpdateZenodoStreamingFiles() throws IOException {
        CmdZenodo cmdZenodo = new CmdZenodo();

        File dataDir = folder.newFolder("streaming-test");
        cmdZenodo.setDataDir(dataDir.getAbsolutePath());
        cmdZenodo.setApiEndpoint("https://sandbox.zenodo.org");
        System.setProperty("ZENODO_TOKEN", ZenodoTestUtil.getAccessToken());

        UUID uuid = UUID.randomUUID();
        IRI contentId = ZenodoTestUtil.contentIdFor(uuid);
        String metadata = ZenodoTestUtil.getMetadataSample(uuid, contentId);

        assertThat(metadata, containsString("{{ ZENODO_DEPOSIT_ID }}"));

        String log = attemptDeposit(cmdZenodo, uuid, contentId, metadata);

        String[] split = StringUtils.split(log, '\n');
        assertThat(split.length, greaterThan(0));
        assertThat(split[split.length - 3], containsString("files/file.txt> <http://purl.org/pav/hasVersion> <hash://md5"));
        String metadataStatement = split[split.length - 2];
        assertThat(metadataStatement, containsString("files/zenodo.json> <http://purl.org/pav/hasVersion> <hash://md5"));
        assertThat(split[split.length - 1], startsWith("<https://sandbox.zenodo.org/records/"));
        assertThat(split[split.length - 1], containsString("> <http://purl.org/pav/lastRefreshedOn>"));


        Quad quad = VersionUtil.parseAsVersionStatementOrNull(metadataStatement);

        assertThat(quad.getSubject().ntriplesString(), endsWith("files/zenodo.json>"));

        Matcher matcher = Pattern.compile(".*/(?<recordId>[0-9]+)/.*").matcher(quad.getSubject().ntriplesString());

        assertTrue(matcher.matches());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        InputStream is = ResourcesHTTP.asInputStream(toIRI("https://sandbox.zenodo.org/api/records/" + matcher.group("recordId")));
        IOUtils.copy(is, baos);

        JsonNode publishedRecordMetadata = new ObjectMapper().readTree(new String(baos.toByteArray(), StandardCharsets.UTF_8));

        assertThat(publishedRecordMetadata.at("/metadata/description").asText(), not(containsString("{{ ZENODO_DEPOSIT_ID }}")));
        assertThat(publishedRecordMetadata.at("/metadata/description").asText(), containsString(matcher.group("recordId") + "/files/review.zip"));

    }

    @Test
    public void depositTwice() throws IOException {
        CmdZenodo cmdZenodo = new CmdZenodo();

        File dataDir = folder.newFolder("streaming-test");
        cmdZenodo.setDataDir(dataDir.getAbsolutePath());
        cmdZenodo.setApiEndpoint("https://sandbox.zenodo.org");
        System.setProperty("ZENODO_TOKEN", ZenodoTestUtil.getAccessToken());

        UUID uuid = UUID.randomUUID();
        IRI contentId = ZenodoTestUtil.contentIdFor(uuid);
        String metadata = ZenodoTestUtil.getMetadataSample(uuid, contentId);

        assertThat(metadata, containsString("{{ ZENODO_DEPOSIT_ID }}"));

        attemptDeposit(cmdZenodo, uuid, contentId, metadata);
        String log = attemptDeposit(cmdZenodo, uuid, contentId, metadata);

        System.out.println(log);

        assertThat(log, not(containsString("HAS_VERSION")));
    }


    public String attemptDeposit(CmdZenodo cmdZenodo, UUID uuid, IRI contentId, String metadata) {
        IRI metadataContentId = Hasher.calcHashIRI(metadata, HashType.md5);

        Stream<Quad> quadStream = Stream.of(
                toStatement(toIRI("https://example.org/file.txt"), HAS_VERSION, contentId),
                toStatement(toIRI("https://example.org/zenodo.json"), HAS_VERSION, metadataContentId)
        );

        String contentStream = quadStream.map(Quad::toString)
                .collect(Collectors.joining("\n"));

        cmdZenodo.setInputStream(IOUtils.toInputStream(contentStream, StandardCharsets.UTF_8));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        cmdZenodo.setOutputStream(outputStream);
        cmdZenodo.setCacheEnabled(false);

        AtomicReference<IRI> requested = new AtomicReference<>(null);
        cmdZenodo.run(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                requested.set(uri);
                return uri.equals(contentId)
                        ? ZenodoTestUtil.dereferencerFor(uuid).get(uri)
                        : IOUtils.toInputStream(metadata, StandardCharsets.UTF_8);
            }
        });
        return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
    }


    @Test
    public void createOrUpdateZenodoTaxodros() throws URISyntaxException, IOException {
        CmdZenodo cmdZenodo = new CmdZenodo();
        String resourceURI = "taxodros-data/6e/f3/6ef3b8e326cd52972da1c00de60dc222";

        File dataDir = folder.newFolder("zenodo-test");
        cmdZenodo.setDataDir(dataDir.getAbsolutePath());
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
        assertThat(split[split.length - 1], startsWith("<https://sandbox.zenodo.org/records/"));
        assertThat(split[split.length - 1], containsString("> <http://www.w3.org/ns/prov#wasDerivedFrom> <line:hash://md5/7e5ae7ff14d66bff5224b21c80cdb87d!/L1> <urn:uuid:"));
    }


    @Test
    public void createDepositAndUpdateToRestrictBatLit() throws URISyntaxException, IOException {
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
    @Test
    public void createDepositWithProvidedLicenseMap() throws URISyntaxException, IOException {
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

    @Test
    public void createDepositAndCreateNewVersionWithDifferentFile() throws URISyntaxException, IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        String resourceOld = "darktaxon-delete/29/cc/29ccb90cb281069a23b4e07ec10583c8";
        String fileChecksumOld = "md5:d3b07384d113edec49eaa6238ad5ff00";
        publishNewVersionOfResource(outputStream, resourceOld);

        assertStateOfDepositedFiles(outputStream, fileChecksumOld);

        outputStream = new ByteArrayOutputStream();
        String resourceNew = "darktaxon-delete/97/4c/974c37be498a377c3795bcbdb6e20581";
        String fileChecksumNew = "md5:c157a79031e1c40f85931829bc5fc552";
        publishNewVersionOfResource(outputStream, resourceNew);

        assertStateOfDepositedFiles(outputStream, fileChecksumNew);


    }

    private void assertStateOfDepositedFiles(ByteArrayOutputStream outputStream, String fileChecksumOld) throws IOException {
        String log = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        String[] split = StringUtils.split(log, '\n');
        assertThat(split.length, greaterThan(0));

        assertThat(log, containsString("https://sandbox.zenodo.org/records/"));

        Pattern compile = Pattern.compile(".*https://sandbox.zenodo.org/records/(?<depositId>[0-9]+).*");
        Matcher matcher = compile.matcher(StringUtils.replace(log, "\n", " "));
        assertThat(matcher.matches(), Is.is(true));


        String depositId = matcher.group("depositId");
        JsonNode depositMetadata = requestDepositMetadata(depositId);
        ZenodoContext ctx = new ZenodoContext("secret", "https://sandbox.zenodo.org");
        ctx.setMetadata(depositMetadata);
        ctx.setDepositId(Long.parseLong(depositId));
        List<IRI> fileEndpoints = ZenodoUtils.getFileEndpoints(ctx);

        assertThat(fileEndpoints.size(), is(1));

        System.out.println(depositMetadata.toPrettyString());

        JsonNode at = depositMetadata.at("/files/0/checksum");
        assertThat(at.isMissingNode(), is(false));
        assertThat(at.asText(), is(fileChecksumOld));
    }

    private void publishNewVersionOfResource(ByteArrayOutputStream outputStream, String resourceOld) throws IOException {
        CmdZenodo cmdZenodo1 = new CmdZenodo();

        URL resource = getClass().getResource(resourceOld);
        URI remote = new File(resource.getFile()).getParentFile().getParentFile().getParentFile().toURI();


        File dataDir = folder.newFolder();
        cmdZenodo1.setDataDir(dataDir.getAbsolutePath());
        cmdZenodo1.setRemotes(Arrays.asList(remote));
        cmdZenodo1.setHashType(HashType.md5);
        cmdZenodo1.setApiEndpoint("https://sandbox.zenodo.org");
        cmdZenodo1.setCreateNewVersionForExisting(true);

        System.setProperty("ZENODO_TOKEN", ZenodoTestUtil.getAccessToken());

        cmdZenodo1.setInputStream(getClass().getResourceAsStream(resourceOld));
        cmdZenodo1.setOutputStream(outputStream);
        cmdZenodo1.setCacheEnabled(false);

        // first make sure a deposit exists
        cmdZenodo1.run();
    }


    private CmdZenodo createCmd(ByteArrayOutputStream outputStream) throws IOException {
        CmdZenodo cmdZenodo = new CmdZenodo();
        String resourceURI = "batlit-data/31/31/3131a4dd8ed099a31e2f2032c0248ba7";

        URL resource = getClass().getResource(resourceURI);
        URI remote = new File(resource.getFile()).getParentFile().getParentFile().getParentFile().toURI();


        File dataDir = folder.newFolder();
        cmdZenodo.setDataDir(dataDir.getAbsolutePath());
        cmdZenodo.setRemotes(Arrays.asList(remote));
        cmdZenodo.setApiEndpoint("https://sandbox.zenodo.org");

        System.setProperty("ZENODO_TOKEN", ZenodoTestUtil.getAccessToken());

        cmdZenodo.setInputStream(getClass().getResourceAsStream(resourceURI));
        cmdZenodo.setOutputStream(outputStream);
        cmdZenodo.setCacheEnabled(false);
        return cmdZenodo;
    }

    private JsonNode requestDepositMetadata(String depositId) throws IOException {
        InputStream inputStream = ResourcesHTTP.asInputStream(toIRI("https://sandbox.zenodo.org/api/records/" + depositId));
        return new ObjectMapper().readTree(inputStream);
    }


}