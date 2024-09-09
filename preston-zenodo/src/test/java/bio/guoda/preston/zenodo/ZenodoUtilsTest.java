package bio.guoda.preston.zenodo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ZenodoUtilsTest {

    @Test
    public void queryForSpecificDeposition() {
        ZenodoContext ctx = new ZenodoContext("secret");
        IRI queryForExistingDepositions = ZenodoUtils.getQueryForExistingDepositions(ctx, Arrays.asList("urn:lsid:biodiversitylibrary.org:part:79807"), "");

        assertThat(
                queryForExistingDepositions.getIRIString(),
                is("https://sandbox.zenodo.org/api/deposit/depositions?q=alternate.identifier:%22urn%3Alsid%3Abiodiversitylibrary.org%3Apart%3A79807%22")
        );
    }

    @Test
    public void htmlSearchResultPageForSpecificDeposition() {
        ZenodoContext ctx = new ZenodoContext("secret");
        IRI queryForExistingDepositions = ZenodoUtils.getSearchPageForExistingRecords(ctx, Arrays.asList("urn:lsid:biodiversitylibrary.org:part:79807"), "");

        assertThat(
                queryForExistingDepositions.getIRIString(),
                is("https://sandbox.zenodo.org/search?q=alternate.identifier:%22urn%3Alsid%3Abiodiversitylibrary.org%3Apart%3A79807%22")
        );
    }

    @Test
    public void queryForExistingDepositions() {
        ZenodoContext ctx = new ZenodoContext("secret");
        IRI queryForExistingDepositions = ZenodoUtils.getQueryForExistingDepositions(ctx, Arrays.asList("foo:bar", "foo:bar"), "");

        assertThat(
                queryForExistingDepositions.getIRIString(),
                is("https://sandbox.zenodo.org/api/deposit/depositions?q=alternate.identifier:%22foo%3Abar%22%20AND%20alternate.identifier:%22foo%3Abar%22")
        );
    }


    @Test
    public void queryForExistingRecords() {
        ZenodoContext ctx = new ZenodoContext("secret", "https://sandbox.zenodo.org", Arrays.asList("community 1", "community 2"));
        IRI queryForExistingDepositions = ZenodoUtils.getQueryForExistingRecords(
                ctx,
                Arrays.asList("foo:bar", "foo:bar"), ""
        );

        assertThat(
                queryForExistingDepositions.getIRIString(),
                is("https://sandbox.zenodo.org/api/records?communities=community%201%2Ccommunity%202&all_versions=false&q=alternate.identifier:%22foo%3Abar%22%20AND%20alternate.identifier:%22foo%3Abar%22")
        );
    }

    @Test
    public void extractFileIds() throws IOException {
        ZenodoContext ctx = getContext();

        InputStream is = getClass().getResourceAsStream("darktaxon-delete/to-be-deleted.json");


        JsonNode jsonNode = new ObjectMapper().readTree(is);

        ctx.setMetadata(jsonNode);
        List<IRI> fileEndpoints = ZenodoUtils.getFileEndpoints(ctx);

        assertThat(fileEndpoints.size(), Is.is(1));
        assertThat(fileEndpoints.get(0).getIRIString(), Is.is("https://sandbox.example.org/api/deposit/depositions/123/files/ba1a0c80-d8a0-4afc-997b-4059b1c03ae5"));
    }

    private ZenodoContext getContext() {
        ZenodoContext ctx = new ZenodoContext("bla", "https://sandbox.example.org");
        ctx.setDepositId(123L);
        return ctx;
    }

    @Test
    public void extractFileIdsNoMatching() throws IOException {
        InputStream is = getClass().getResourceAsStream("darktaxon-delete/to-be-deleted-no-files.json");

        JsonNode jsonNode = new ObjectMapper().readTree(is);
        ZenodoContext context = getContext();
        context.setMetadata(jsonNode);
        List<IRI> fileEndpoints = ZenodoUtils.getFileEndpoints(context);

        assertThat(fileEndpoints.size(), Is.is(0));

    }

}