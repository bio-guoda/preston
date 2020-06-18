package bio.guoda.preston.store;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Predicate;

public class KeyValueStoreProtocolAware implements KeyValueStoreReadOnly {
    private final Map<String, KeyValueStoreReadOnly> prefixMap;

    public KeyValueStoreProtocolAware(Map<String, KeyValueStoreReadOnly> prefixMap) {
        this.prefixMap = new TreeMap<>(prefixMap);
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        Optional<KeyValueStoreReadOnly> matchingStore = prefixMap.entrySet()
                .stream()
                .filter(prefix -> StringUtils.startsWith(StringUtils.lowerCase(key.getIRIString()), StringUtils.lowerCase(prefix.getKey())))
                .findFirst()
                .map(Map.Entry::getValue);

        return matchingStore
                .orElse(key1 -> null)
                .get(key);
    }
}
