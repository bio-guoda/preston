package bio.guoda.preston;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class QuadMatcher extends TypeSafeMatcher<Quad> {

    private final Quad quad;

    private QuadMatcher(Quad quad) {
        this.quad = quad;
    }

    @Override
    protected boolean matchesSafely(Quad item) {
        return quad != null
                && item.getGraphName().equals(quad.getGraphName())
                && StringUtils.equals(item.getSubject().ntriplesString(), quad.getSubject().ntriplesString())
                && StringUtils.equals(item.getPredicate().ntriplesString(), quad.getPredicate().ntriplesString())
                && StringUtils.equals(item.getObject().ntriplesString(), quad.getObject().ntriplesString());
    }

    @Override
    public void describeTo(Description description) {

    }

    @Factory
    public static Matcher<Quad> hasQuad(Quad quad) {
        return new QuadMatcher(quad);
    }

}
