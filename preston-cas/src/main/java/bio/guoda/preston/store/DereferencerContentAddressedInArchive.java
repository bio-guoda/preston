package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

public class DereferencerContentAddressedInArchive implements Dereferencer<InputStream> {
    private final Dereferencer<InputStream> dereferencer;

    private final BlobStore blobStore;

    public DereferencerContentAddressedInArchive(Dereferencer<InputStream> dereferencer) {
        this(dereferencer, null);
    }

    public DereferencerContentAddressedInArchive(Dereferencer<InputStream> dereferencer, BlobStore blobStore) {
        this.dereferencer = dereferencer;
        this.blobStore = blobStore;
    }

    @Override
    public InputStream get(IRI uri) throws IOException {
        String iriString = uri == null ? null : uri.getIRIString();
        InputStream inputStream = null;
        if (StringUtils.startsWith(iriString, "tgz:")
                || StringUtils.startsWith(iriString, "zip:")) {
            String iriSuffix = StringUtils.substring(iriString, 4);
            String[] tarUrlSplit = iriSuffix == null ? new String[]{} : iriSuffix.split("!/");
            if (tarUrlSplit.length == 2) {
                String archiveURL = tarUrlSplit[0];
                String hashPath = tarUrlSplit[1];
                IRI expectedHashIRI = extractHashURI(hashPath);
                if (expectedHashIRI != null) {
                    InputStream data = dereferencer == null
                            ? null
                            : dereferencer.get(RefNodeFactory.toIRI(archiveURL));
                    if (data != null) {
                        if (StringUtils.startsWith(iriString, "tgz:")) {
                            inputStream = getInputStreamTarGz(data, expectedHashIRI, inputStream);
                        } else if (StringUtils.startsWith(iriString, "zip:")) {
                            inputStream = getInputStreamZip(data, expectedHashIRI, inputStream);
                        }
                    }
                }
            }
        }
        return inputStream;
    }

    private InputStream getInputStreamZip(InputStream data, IRI expectedHashIRI, InputStream inputStream) throws IOException {
        ZipArchiveInputStream tarInputStream = new ZipArchiveInputStream(data);
        ZipArchiveEntry entry;
        while ((entry = tarInputStream.getNextEntry()) != null) {
            if (!entry.isDirectory()) {
                if (blobStore == null) {
                    IRI foundHashIRI = extractHashURI(entry.getName());
                    if (foundHashIRI != null && foundHashIRI.equals(expectedHashIRI)) {
                        inputStream = tarInputStream;
                        break;
                    }
                } else {
                    blobStore.put(tarInputStream);
                }
            }
        }
        if (blobStore != null) {
            inputStream = blobStore.get(expectedHashIRI);
        }
        return inputStream;
    }

    private InputStream getInputStreamTarGz(InputStream data, IRI expectedHashIRI, InputStream inputStream) throws IOException {
        TarArchiveInputStream tarInputStream = new TarArchiveInputStream(new GZIPInputStream(data));
        TarArchiveEntry entry;
        while ((entry = tarInputStream.getNextEntry()) != null) {
            if (entry.isFile()) {
                if (blobStore == null) {
                    IRI foundHashIRI = extractHashURI(entry.getName());
                    if (foundHashIRI != null && foundHashIRI.equals(expectedHashIRI)) {
                        inputStream = tarInputStream;
                        break;
                    }
                } else {
                    blobStore.put(tarInputStream);
                }
            }
        }
        if (blobStore != null) {
            inputStream = blobStore.get(expectedHashIRI);
        }
        return inputStream;
    }

    public static IRI extractHashURI(String hashPath) {
        String[] hashPathElem = hashPath.split("/");
        String lastHashElem = hashPathElem[hashPathElem.length - 1];

        Optional<HashType> hashType = Arrays.stream(HashType.values())
                .filter(type -> type.getHexLength() == StringUtils.length(lastHashElem))
                .filter(type -> type.getHexPattern().matcher(lastHashElem).matches())
                .findFirst();

        return hashType
                .map(type -> RefNodeFactory.toIRI(type.getPrefix() + lastHashElem))
                .orElse(null);
    }

}
