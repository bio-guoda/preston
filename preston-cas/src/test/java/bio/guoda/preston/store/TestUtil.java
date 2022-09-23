package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

public class TestUtil {
    public static String toUTF8(InputStream content) throws IOException {
        return content == null ? null : IOUtils.toString(content, StandardCharsets.UTF_8);
    }

    public static KeyValueStoreWithRemove getTestPersistence() {
        return new KeyValueStoreWithRemove() {
            private final Map<String, String> lookup = new TreeMap<>();

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                IRI key = keyGeneratingStream.generateKeyWhileStreaming(is, os);
                ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray());
                put(key, bais);
                return key;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {
                lookup.putIfAbsent(key.getIRIString(), toUTF8(is));
            }

            @Override
            public InputStream get(IRI key) throws IOException {
                String input = lookup.get(key.getIRIString());
                return input == null ? null : IOUtils.toInputStream(input, StandardCharsets.UTF_8);
            }

            @Override
            public void remove(IRI key) throws IOException {
                lookup.remove(key.getIRIString());
            }

        };
    }

    public static BlobStoreReadOnly getTestBlobStore(HashType type) {
        return new BlobStoreAppendOnly(getTestPersistence(), true, type);
    }

    public static BlobStoreReadOnly getTestBlobStoreForResource(String pathToResource) {
        return new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                return getClass().getResourceAsStream(pathToResource);
            }
        };
    }

    public static InputStream filterLineFeedFromTextInputStream(InputStream is) throws IOException {
        // Git client for windows is configured to insert \r (carriage returns) when checking out text files ; See e.g., https://github.com/nodejs/node/pull/20754
        // this causes issues when using hashes that reply on byte sequences.
        // https://help.github.com/en/github/using-git/configuring-git-to-handle-line-endings
        String s = IOUtils.toString(is, StandardCharsets.UTF_8);
        String replace = StringUtils.replace(s, "\r\n", "\n");
        return IOUtils.toInputStream(replace, StandardCharsets.UTF_8);
    }

}
