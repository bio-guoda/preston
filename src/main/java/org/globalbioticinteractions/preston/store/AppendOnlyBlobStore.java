package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.globalbioticinteractions.preston.Hasher;
import org.globalbioticinteractions.preston.RDFUtil;
import org.globalbioticinteractions.preston.model.RefNodeFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;


public class AppendOnlyBlobStore implements BlobStore {

    private final Persistence persistence;

    public AppendOnlyBlobStore(Persistence persistence) {
        this.persistence = persistence;
    }

    // write-once, read-many

    @Override
    public IRI putBlob(InputStream is) throws IOException {
        return RefNodeFactory.toIRI(URI.create(persistence.put((is1, os1)-> {
            IRI key = Hasher.calcSHA256(is1, os1);
            return key.getIRIString();
        }, is)));
    }

    @Override
    public IRI putBlob(RDFTerm entity) throws IOException {
        String value = RDFUtil.getValueFor(entity);
        return putBlob(IOUtils.toInputStream(value, StandardCharsets.UTF_8));
    }

    @Override
    public InputStream get(IRI key) throws IOException {
        return key == null ? null : persistence.get(key.getIRIString());
    }

}
