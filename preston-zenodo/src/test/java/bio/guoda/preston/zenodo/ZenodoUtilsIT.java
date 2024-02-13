package bio.guoda.preston.zenodo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;

public class ZenodoUtilsIT {

    public static final String CONTENT_ID_PDF = "hash://md5/639988a4074ded5208a575b760a5dc5e";
    public static final String TAXODROS_ID = "urn:lsid:taxodros.uzh.ch:id:abd%20el-halim%20et%20al.,%202005";

    private ZenodoContext ctx = null;

    @Before
    public void create() throws IOException {
        InputStream request = getInputStream();
        ObjectMapper objectMapper = ZenodoUtils.getObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(IOUtils.toString(request, StandardCharsets.UTF_8));
        ctx = new ZenodoContext(getAccessToken(), "https://sandbox.zenodo.org");
        ctx = ZenodoUtils.create(ctx, jsonNode);

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
        Collection<Pair<Long, String>> byAlternateIds = ZenodoUtils.findByAlternateIds(ctx, Arrays.asList(CONTENT_ID_PDF, TAXODROS_ID));
        byAlternateIds
                .stream()
                .filter(d -> StringUtils.equals(d.getValue(), "unsubmitted"))
                .map(Pair::getKey)
                .forEach(depositId -> {
                    ZenodoContext ctx = new ZenodoContext(this.ctx.getAccessToken());
                    ctx.setDepositId(depositId);
                    try {
                        ZenodoUtils.delete(ctx);
                    } catch (IOException e) {
                        // ignore
                    }
                });
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

        Collection<Pair<Long, String>> ids = ZenodoUtils.findByAlternateIds(ctx, contentId);
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
        ZenodoUtils.upload(this.ctx, "some spacey name.json", resourceAsStream);
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
    public void createNewVersion() throws IOException {

        InputStream resourceAsStream = getInputStream();

        assertNotNull(resourceAsStream);

        ZenodoUtils.upload(this.ctx, "some spacey name.json", resourceAsStream);

        ZenodoUtils.publish(this.ctx);
        Long depositIdPrevious = ctx.getDepositId();
        ctx = ZenodoUtils.createNewVersion(this.ctx);
        assertThat(ctx.getDepositId(), is(notNullValue()));
        assertThat(ctx.getDepositId(), is(greaterThan(depositIdPrevious)));

    }


    private String getAccessToken() throws IOException {
        return IOUtils.toString(getClass().getResourceAsStream("zenodo-token.hidden"), StandardCharsets.UTF_8);
    }


}
