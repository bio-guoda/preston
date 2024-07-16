package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamHandlerUtil {

    public static void writeRecord(AtomicBoolean foundAtLeastOne, ObjectNode objectNode, OutputStream outputStream) throws IOException {
        ObjectNode metadata = new ObjectMapper().createObjectNode();
        metadata.set("metadata", objectNode);
        IOUtils.copy(IOUtils.toInputStream(metadata.toString(), StandardCharsets.UTF_8), outputStream);
        IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
        objectNode.removeAll();
        foundAtLeastOne.set(true);
    }

    public static String makeActionable(String providedContentId) {
        return "https://linker.bio/" + providedContentId;
    }

    public static void appendContentId(ObjectNode objectNode, String downloadUrl, HashType hashType, Dereferencer<InputStream> dereferencer, Persisting persisting) throws ContentStreamException {
        if (StringUtils.isNotBlank(downloadUrl)) {
            try {
                IRI downloadIRI = RefNodeFactory.toIRI(downloadUrl);
                InputStream attachementInputStream = ContentQueryUtil.getContent(dereferencer, downloadIRI, persisting);
                if (attachementInputStream == null) {
                    throw new ContentStreamException("cannot generate Zenodo record due to unresolved attachment [" + downloadUrl + "]");
                }
                IRI contentId = Hasher.calcHashIRI(
                        attachementInputStream,
                        NullOutputStream.INSTANCE,
                        hashType
                );
                ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.HAS_VERSION, makeActionable(contentId.getIRIString()));
            } catch (IOException e) {
                throw new ContentStreamException("cannot generate Zenodo record due to unresolved attachment [" + downloadUrl + "]", e);
            }
        }
    }
}
