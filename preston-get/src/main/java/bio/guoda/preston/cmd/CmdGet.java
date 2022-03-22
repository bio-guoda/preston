package bio.guoda.preston.cmd;

import bio.guoda.preston.IRIFixingProcessor;
import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.VersionUtil;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeFactory.toIRI;

@Parameters(separators = "= ", commandDescription = "get biodiversity data")
public class CmdGet extends Persisting implements Runnable {

    @Parameter(description = "content ids or known aliases (e.g., [hash://sha256/8ed311...])",
            validateWith = URIValidator.class)
    private List<String> contentIdsOrAliases = new ArrayList<>();

    private OutputStream outputStream = System.out;

    @Override
    public void run() {
        BlobStoreReadOnly blobStore = new BlobStoreAppendOnly(
                getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory())
        );
        run(blobStore);
    }

    public void run(BlobStoreReadOnly blobStore) {
        run(blobStore, contentIdsOrAliases);
    }

    void run(BlobStoreReadOnly blobStore, List<String> contentIdsOrAliases) {
        try {
            if (contentIdsOrAliases.isEmpty()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                String line;
                while ((line = reader.readLine()) != null) {
                    Quad quad = RDFUtil.asQuad(line);
                    if (quad  != null) {
                        IRI version = VersionUtil.mostRecentVersionForStatement(quad);
                        if (version != null) {
                            handleContentQuery(blobStore, version.getIRIString());
                        }
                    } else {
                        handleContentQuery(blobStore, StringUtils.trim(line));
                    }
                }
            } else {
                for (String s : contentIdsOrAliases) {
                    handleContentQuery(blobStore, s);
                }
            }
        } catch (Throwable th) {
            th.printStackTrace(System.err);
            throw new RuntimeException(th);
        }
    }

    private void handleContentQuery(BlobStoreReadOnly blobStore, String queryString) throws IOException {
        IRI queryIRI = toIRI(queryString);

        BlobStoreReadOnly query = resolvingBlobStore(blobStore);

        try {
            InputStream contentStream = query.get(
                    new IRIFixingProcessor()
                            .process(queryIRI)
            );
            if (contentStream == null) {
                throw new IOException("[" + queryString + "] not found.");
            }
            IOUtils.copyLarge(contentStream, getOutputStream());
        } catch (IOException e) {
            throw new IOException("problem retrieving [" + queryString + "]", e);
        }
    }

    public List<String> getContentIdsOrAliases() {
        return contentIdsOrAliases;
    }

    public void setContentIdsOrAliases(List<String> contentIdsOrAliases) {
        this.contentIdsOrAliases = contentIdsOrAliases;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public void setOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
    }


}
