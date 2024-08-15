package bio.guoda.preston.zenodo;

import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ZenodoUtilsTest {

    @Test
    public void queryForExistingDepositions() {
        ZenodoContext ctx = new ZenodoContext("secret");
        IRI queryForExistingDepositions = ZenodoUtils.getQueryForExistingDepositions(ctx, Arrays.asList("foo:bar", "foo:bar"));

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
                Arrays.asList("foo:bar", "foo:bar")
        );

        assertThat(
                queryForExistingDepositions.getIRIString(),
                is("https://sandbox.zenodo.org/api/records?communities=community%201%2Ccommunity%202&all_versions=false&q=alternate.identifier:%22foo%3Abar%22%20AND%20alternate.identifier:%22foo%3Abar%22")
        );
    }

}