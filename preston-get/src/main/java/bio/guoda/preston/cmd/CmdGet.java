package bio.guoda.preston.cmd;

import bio.guoda.preston.IRIFixingProcessor;
import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeFactory.toIRI;

@CommandLine.Command(
        name = "cat",
        aliases = {"get"},
        description = CmdGet.GET_BIODIVERSITY_DATA
)
public class CmdGet extends Persisting implements Runnable {

    public static final String CONTENT_ID = "Content ids or known aliases (e.g., [hash://sha256/8ed311...])";
    public static final String GET_BIODIVERSITY_DATA = "Get biodiversity data";
    @CommandLine.Parameters(description = CONTENT_ID)
    private List<IRI> contentIdsOrAliases = new ArrayList<>();

    @Override
    public void run() {
        BlobStoreReadOnly blobStore = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory(getHashType())), true, getHashType());
        run(blobStore);
    }

    public void run(BlobStoreReadOnly blobStore) {
        run(blobStore, contentIdsOrAliases);
    }

    void run(BlobStoreReadOnly blobStore, List<IRI> contentIdsOrAliases) {
        try {
            if (contentIdsOrAliases.isEmpty()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(getInputStream()));
                String line;
                while ((line = reader.readLine()) != null) {
                    Quad quad = RDFUtil.asQuad(line);
                    if (quad == null) {
                        IRI queryIRI = toIRI(StringUtils.trim(line));
                        quad = RefNodeFactory.toStatement(RefNodeFactory.toBlank(), RefNodeConstants.HAS_VERSION, queryIRI);
                    }
                    handleContentQuery(blobStore, quad);
                }
            } else {
                for (IRI s : contentIdsOrAliases) {
                    handleContentQuery(blobStore, s);
                }
            }
        } catch (Throwable th) {
            th.printStackTrace(System.err);
            throw new RuntimeException(th);
        }
    }

    private void handleContentQuery(BlobStoreReadOnly blobStore, Quad quad) throws IOException {
        IRI version = VersionUtil.mostRecentVersionForStatement(quad);
        if (version != null) {
            handleContentQuery(blobStore, version);
        }
    }

    private void handleContentQuery(BlobStoreReadOnly blobStore, IRI queryIRI) throws IOException {

        BlobStoreReadOnly query = resolvingBlobStore(blobStore);

        try {
            InputStream contentStream = query.get(
                    new IRIFixingProcessor()
                            .process(queryIRI)
            );
            if (contentStream == null) {
                throw new IOException("[" + queryIRI.getIRIString() + "] not found.");
            }
            IOUtils.copyLarge(contentStream, getOutputStream());
        } catch (IOException e) {
            throw new IOException("problem retrieving [" + queryIRI.getIRIString() + "]", e);
        }
    }

    public List<IRI> getContentIdsOrAliases() {
        return contentIdsOrAliases;
    }

    public void setContentIdsOrAliases(List<IRI> contentIdsOrAliases) {
        this.contentIdsOrAliases = contentIdsOrAliases;
    }


}
