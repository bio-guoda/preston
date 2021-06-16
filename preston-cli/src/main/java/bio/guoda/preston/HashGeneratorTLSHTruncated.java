package bio.guoda.preston;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HashGeneratorTLSHTruncated extends HashGeneratorAbstract<IRI> {

    @Override
    public IRI hash(InputStream is, OutputStream os, boolean shouldCloseInputStream) throws IOException {
        String hexEncodedHash = TLSHUtil.calculateLTSH(is, os, shouldCloseInputStream);
        return Hasher.toHashIRI(HashType.tlsh, StringUtils.substring(hexEncodedHash, 3));
    }

}
