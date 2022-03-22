package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;


public class HashTypeTest {

    @Test
    public void hashLengthSHA256() {
        IRI iri = Hasher.calcHashIRI("bla", HashType.sha256);
        assertThat(iri.getIRIString().length(), Is.is(78));
        assertThat(HashType.sha256.getIriStringLength(), Is.is(78));

    }

   @Test
    public void hashLengthMD5() {
        IRI iri = Hasher.calcHashIRI("bla", HashType.md5);
        assertThat(iri.getIRIString().length(), Is.is(43));
    }

}