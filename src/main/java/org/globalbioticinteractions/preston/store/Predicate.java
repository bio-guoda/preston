package org.globalbioticinteractions.preston.store;

import java.net.URI;

public final class Predicate {
    public final static URI HAS_CONTENT = URI.create("http://example.com/hasContent");
    public final static URI HAS_CONTENT_HASH = URI.create("http://example.com/hasSHA256");

    public static final URI HAS_REGISTRY = URI.create("http://example.com/hasRegistry");
    public static final URI SUCCEEDED_BY = URI.create("http://example.com/succeededBy");
}
