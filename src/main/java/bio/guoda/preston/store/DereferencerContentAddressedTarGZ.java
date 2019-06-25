package bio.guoda.preston.store;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class DereferencerContentAddressedTarGZ implements Dereferencer<IRI> {
    private final Dereferencer<InputStream> dereferencer;
    private final BlobStore blobStore;

    public DereferencerContentAddressedTarGZ(Dereferencer<InputStream> dereferencer, BlobStore blobStore) {
        this.dereferencer = dereferencer;
        this.blobStore = blobStore;
    }

    @Override
    public IRI dereference(IRI uri) throws IOException {
        String iriString = uri == null ? null : uri.getIRIString();
        IRI dereferenced = null;
        if (StringUtils.startsWith(iriString, "tgz:")) {
            String[] tarUrlSplit = iriString.substring(4, iriString.length()).split("!/");
            if (tarUrlSplit.length == 2) {
                String archiveURL = tarUrlSplit[0];
                String hashPath = tarUrlSplit[1];
                String[] hashPathElem = hashPath.split("/");
                String lastHashElem = hashPathElem[hashPathElem.length - 1];
                String expectedHashKey = "hash://sha256/" + lastHashElem;
                if (HashKeyUtil.isValidHashKey(expectedHashKey)) {
                    try (InputStream data = dereferencer == null
                            ? null
                            : dereferencer.dereference(RefNodeFactory.toIRI(archiveURL))) {
                        if (data != null) {
                            IRI expectedHashIRI = RefNodeFactory.toIRI(expectedHashKey);
                            TarArchiveInputStream tarInputStream = new TarArchiveInputStream(new GZIPInputStream(data));
                            TarArchiveEntry entry = null;
                            while ((entry = tarInputStream.getNextTarEntry()) != null) {
                                if (entry.isFile()) {
                                    IRI iri = blobStore.putBlob(tarInputStream);
                                    if (iri != null && iri.equals(expectedHashIRI)) {
                                        dereferenced = expectedHashIRI;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return dereferenced;
    }

}
