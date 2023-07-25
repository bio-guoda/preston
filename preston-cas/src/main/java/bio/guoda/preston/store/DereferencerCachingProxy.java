package bio.guoda.preston.store;

import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.util.LinkedHashMap;

public class DereferencerCachingProxy implements Dereferencer<IRI> {
    private final Dereferencer<IRI> dereferencer;
    private final LRUMap<IRI, IRI> cache;

    public DereferencerCachingProxy(
            Dereferencer<IRI> dereferencer) {
        this(dereferencer, 4096);
    }

    DereferencerCachingProxy(
            Dereferencer<IRI> dereferencer, int cacheSize) {
        this.dereferencer = dereferencer;
        this.cache = new LRUMap<>(cacheSize);
    }

    @Override
    public IRI get(IRI locationId) throws IOException {
        IRI contentId = cache.get(locationId);
        if (contentId == null) {
            contentId = dereferencer.get(locationId);
            cache.put(locationId, contentId);
        }
        return contentId;
    }

}
