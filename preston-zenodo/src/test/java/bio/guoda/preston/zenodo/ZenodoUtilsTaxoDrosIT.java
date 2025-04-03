package bio.guoda.preston.zenodo;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.store.Dereferencer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.http.HttpEntity;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.InputStreamEntity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class ZenodoUtilsTaxoDrosIT {

    public static final String CONTENT_ID_PDF = "hash://md5/639988a4074ded5208a575b760a5dc5e";
    public static final String TAXODROS_ID = "urn:lsid:taxodros.uzh.ch:id:abd%20el-halim%20et%20al.,%202005";

    private ZenodoContext ctx = null;

    @Before
    public void create() throws IOException {
        ctx = ZenodoTestUtil.createSandboxContext();
        InputStream request = getInputStream();
        assertNotNull(request);
        ctx = ZenodoUtils.createEmptyDeposit(ctx);
        ZenodoUtils.update(ctx, IOUtils.toString(request, StandardCharsets.UTF_8));

        assertNotNull(ctx);
        assertNotNull(ctx.getBucketId());
        assertNotNull(ctx.getDepositId());

    }

    @After
    public void delete() throws IOException {
        try {
            ZenodoUtils.delete(this.ctx);
            cleanupPreExisting();
        } catch (IOException ex) {
            // ignore
        }
    }

    private void cleanupPreExisting() throws IOException {
        Collection<Pair<Long, String>> byAlternateIds = ZenodoUtils.findRecordsByAlternateIds(ctx, Arrays.asList(getContentId(), getLsid()), "", uri -> ResourcesHTTP.asInputStream(uri));
        byAlternateIds
                .stream()
                .filter(d -> StringUtils.equals(d.getValue(), "unsubmitted"))
                .map(Pair::getKey)
                .forEach(depositId -> {
                    ZenodoContext ctx = new ZenodoContext(this.ctx.getAccessToken(), this.ctx.getEndpoint());
                    ctx.setDepositId(depositId);
                    try {
                        ZenodoUtils.delete(ctx);
                    } catch (IOException e) {
                        // ignore
                    }
                });
    }

    private String getContentId() {
        return CONTENT_ID_PDF;
    }


    @Test
    public void findDepositByHash() throws IOException {
        assertOneRecordWithMatchingId(Arrays.asList(getContentId()));
    }

    @Test
    public void findDepositByTaxoDrosId() throws IOException {
        assertOneRecordWithMatchingId(Arrays.asList(getLsid()));
    }

    private String getLsid() {
        return TAXODROS_ID;
    }

    @Test
    public void findDepositByBothContentIdAndTaxoDrosId() throws IOException {
        assertOneRecordWithMatchingId(Arrays.asList(getContentId(), getLsid()));
    }

    private void assertOneRecordWithMatchingId(List<String> contentId) throws IOException {

        Collection<Pair<Long, String>> ids = ZenodoUtils.findRecordsByAlternateIds(ctx, contentId, "", uri -> ResourcesHTTP.asInputStream(uri));
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
    public void uploadDataUsingFileEntity() throws IOException, URISyntaxException {
        File file = new File(getClass().getResource("zenodo-metadata.json").toURI());
        FileEntity entity = new FileEntity(file);
        assertTrue(entity.isRepeatable());
        assertFalse(entity.isStreaming());
        assertFalse(entity.isChunked());
        assertUpload(entity, expectedContentId(), "some spacey name.json");
    }

    @Test
    public void uploadDataUsingStreamEntity() throws IOException {
        InputStreamEntity entity = new InputStreamEntity(getInputStream());
        assertFalse(entity.isRepeatable());
        assertTrue(entity.isStreaming());
        assertFalse(entity.isChunked());
        // stream entities not supported somehow, but accepted as 0 length content
        assertUpload(entity, contentIdZeroLengthContent(), "some spacey name.json");
    }

    private String expectedContentId() throws IOException {
        return Hasher.calcHashIRI(getInputStream(), NullOutputStream.INSTANCE, HashType.md5).getIRIString();
    }


    @Test
    public void uploadDataUsingDereferencingEntity() throws IOException {
        Dereferencer<InputStream> dereferencer = new Dereferencer<InputStream>() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                return getInputStream();
            }
        };

        HttpEntity entity = new DereferencingEntity(dereferencer, RefNodeFactory.toIRI("foo:bar"));
        assertTrue(entity.isRepeatable());
        assertTrue(entity.isStreaming());
        assertFalse(entity.isChunked());
        assertUpload(entity, expectedContentId(), "some spacey name.json");
    }

    @Test
    public void uploadDataUsingDereferencingEntityWithAmpersandAndComma() throws IOException {
        Dereferencer<InputStream> dereferencer = uri -> getClass().getResourceAsStream("zenodo-metadata.json");

        HttpEntity entity = new DereferencingEntity(dereferencer, RefNodeFactory.toIRI("foo:bar"));
        assertTrue(entity.isRepeatable());
        assertTrue(entity.isStreaming());
        assertFalse(entity.isChunked());
        assertUpload(entity, expectedContentId(), "some space & name , comma.json");
    }

    private String contentIdZeroLengthContent() throws IOException {
        return Hasher.calcHashIRI(IOUtils.toInputStream("", StandardCharsets.UTF_8), NullOutputStream.INSTANCE, HashType.md5).getIRIString();
    }

    private void assertUpload(HttpEntity entity, String expectedChecksum, String filename) throws IOException {
        ZenodoContext uploadContext = ZenodoUtils.upload(this.ctx, filename, entity);
        JsonNode checksum = uploadContext.getMetadata().at("/checksum");
        if (checksum != null) {
            String actualChecksum = checksum.asText();
            assertThat("hash://md5/" + StringUtils.substring(actualChecksum, 4), is(expectedChecksum));
        } else {
            fail("no file found");
        }
    }


    private long getContentLength() {
        return new CountingOutputStream(NullOutputStream.INSTANCE).getByteCount();
    }

    private InputStream getInputStream() {
        return getClass().getResourceAsStream("zenodo-metadata.json");
    }

    @Test
    public void updateMetadata() throws IOException {
        InputStream inputStream = getInputStream();
        JsonNode payload = ZenodoUtils.getObjectMapper().readTree(inputStream);
        String input = ZenodoUtils.getObjectMapper().writer().writeValueAsString(payload);
        ZenodoUtils.update(this.ctx, input);
    }

    @Test
    public void updateMetadataWithKeywords() throws IOException {
        InputStream inputStream = getClass().getResourceAsStream("zenodo-metadata-with-keywords.json");
        JsonNode payload = ZenodoUtils.getObjectMapper().readTree(inputStream);
        String input = ZenodoUtils.getObjectMapper().writer().writeValueAsString(payload);
        ZenodoUtils.update(this.ctx, input);
    }

    @Test
    public void createNewVersion() throws IOException {

        InputStream resourceAsStream = getInputStream();

        assertNotNull(resourceAsStream);

        Dereferencer<InputStream> dereferencer = uri -> getInputStream();

        ZenodoUtils.upload(this.ctx,
                "some spacey & name.json",
                new DereferencingEntity(dereferencer, RefNodeFactory.toIRI("foo:bar"))
        );

        ZenodoUtils.publish(this.ctx);


        Long depositIdPrevious = ctx.getDepositId();
        ctx = ZenodoUtils.createNewVersion(this.ctx);
        assertThat(ctx.getDepositId(), is(notNullValue()));
        assertThat(ctx.getDepositId(), is(greaterThan(depositIdPrevious)));

    }


    @Test
    public void findDepositByBothContentIdAndTaxoDrosIdPickMostRecentVersion() throws IOException {

        List<String> contentIds = Arrays.asList(
                "hash://md5/8ec637e349f8bbce57cd0054d20d9d8f",
                "urn:lsid:taxodros.uzh.ch:id:aboul-nasr,%201954b");
        Collection<Pair<Long, String>> ids = ZenodoUtils.findRecordsByAlternateIds(ctx, contentIds, "", uri -> ResourcesHTTP.asInputStream(uri));
        assertThat(ids, not(nullValue()));
        List<Long> filteredIds = ids
                .stream()
                .map(Pair::getKey)
                .distinct()
                .collect(Collectors.toList());
        assertThat(filteredIds.size(), is(1));
        assertThat(filteredIds.get(0), is(28657L));


    }


}
