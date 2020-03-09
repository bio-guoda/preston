package bio.guoda.preston;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class QuadMatcher extends TypeSafeMatcher<Quad> {

    private final Quad triple;

    private QuadMatcher(Quad triple) {
        this.triple = triple;
    }

    @Override
    protected boolean matchesSafely(Quad item) {
        return triple != null
                && item.getGraphName().equals(triple.getGraphName())
                && StringUtils.equals(item.getSubject().ntriplesString(), triple.getSubject().ntriplesString())
                && StringUtils.equals(item.getPredicate().ntriplesString(), triple.getPredicate().ntriplesString())
                && StringUtils.equals(item.getObject().ntriplesString(), triple.getObject().ntriplesString());
    }

    @Override
    public void describeTo(Description description) {

    }

    @Factory
    public static Matcher<Quad> hasQuad(Quad triple) {
        return new QuadMatcher(triple);
    }

}
