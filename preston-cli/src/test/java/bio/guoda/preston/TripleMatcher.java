package bio.guoda.preston;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.TripleLike;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class TripleMatcher extends TypeSafeMatcher<TripleLike> {

    private final TripleLike triple;

    private TripleMatcher(TripleLike triple) {
        this.triple = triple;
    }

    @Override
    protected boolean matchesSafely(TripleLike item) {
        return triple != null
                && StringUtils.equals(item.getSubject().ntriplesString(), triple.getSubject().ntriplesString())
                && StringUtils.equals(item.getPredicate().ntriplesString(), triple.getPredicate().ntriplesString())
                && StringUtils.equals(item.getObject().ntriplesString(), triple.getObject().ntriplesString());
    }

    @Override
    public void describeTo(Description description) {

    }

    @Factory
    public static Matcher<TripleLike> hasTriple(TripleLike triple) {
        return new TripleMatcher(triple);
    }

}
