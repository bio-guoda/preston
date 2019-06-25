package bio.guoda.preston.store;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class DereferencerContentAddressedTarGZ implements Dereferencer<InputStream> {
    private final Dereferencer<InputStream> dereferencer;

    public DereferencerContentAddressedTarGZ(Dereferencer<InputStream> dereferencer) {
        this.dereferencer = dereferencer;
    }

    @Override
    public InputStream dereference(IRI uri) throws IOException {
        String iriString = uri == null ? null : uri.getIRIString();
        InputStream inputStream = null;
        if (StringUtils.startsWith(iriString, "tgz:")) {
            String iriSuffix = StringUtils.substring(iriString, 4);
            String[] tarUrlSplit = iriSuffix == null ? new String[]{} : iriSuffix.split("!/");
            if (tarUrlSplit.length == 2) {
                String archiveURL = tarUrlSplit[0];
                String hashPath = tarUrlSplit[1];
                IRI expectedHashIRI = extractHashURI(hashPath);
                if (expectedHashIRI != null) {
                    InputStream data = dereferencer == null
                            ? null
                            : dereferencer.dereference(RefNodeFactory.toIRI(archiveURL));
                    if (data != null) {
                        TarArchiveInputStream tarInputStream = new TarArchiveInputStream(new GZIPInputStream(data));
                        TarArchiveEntry entry;
                        while ((entry = tarInputStream.getNextTarEntry()) != null) {
                            if (entry.isFile()) {
                                IRI foundHashIRI = extractHashURI(entry.getName());
                                if (foundHashIRI != null && foundHashIRI.equals(expectedHashIRI)) {
                                    inputStream = tarInputStream;
                                    break;
                                }
                            }
                        }
                    }

                }
            }
        }
        return inputStream;
    }

    public IRI extractHashURI(String hashPath) {
        String[] hashPathElem = hashPath.split("/");
        String lastHashElem = hashPathElem[hashPathElem.length - 1];

        String hashString = "hash://sha256/" + lastHashElem;
        return HashKeyUtil.isValidHashKey(hashString)
                ? RefNodeFactory.toIRI(hashString)
                : null;
    }

}
