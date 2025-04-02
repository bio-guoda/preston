package bio.guoda.preston.zenodo;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.store.Dereferencer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class ZenodoTestUtil {
    public static String getAccessToken() throws IOException {
        return IOUtils.toString(ZenodoUtilsTaxoDrosIT.class.getResourceAsStream("zenodo-token.hidden"), StandardCharsets.UTF_8);
    }

    public static String getMetadataSample(UUID uuid, IRI contentId) throws IOException {
        return StringUtils.replace(
                StringUtils.replace(IOUtils.toString(ZenodoTestUtil.class.getResourceAsStream("zenodo-metadata-globi-review.json"), StandardCharsets.UTF_8),
        "hash://md5/foo", contentId.getIRIString())
        , "urn:lsid:foo", "urn:lsid:" + uuid.toString());
    }

    public static Dereferencer<InputStream> dereferencerFor(UUID uuid) {
        return new Dereferencer<InputStream>() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return inputStreamFrom(uuid);
            }
        };
    }

    public static ByteArrayInputStream inputStreamFrom(UUID uuid) {
        return new ByteArrayInputStream(uuid.toString().getBytes(StandardCharsets.UTF_8));
    }

    public static IRI contentIdFor(UUID uuid) throws IOException {
        return Hasher.calcHashIRI(inputStreamFrom(uuid), NullOutputStream.INSTANCE, HashType.md5);
    }
}
