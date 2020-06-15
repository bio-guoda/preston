package bio.guoda.preston.cmd;

import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.RegistryReaderALA;
import bio.guoda.preston.process.RegistryReaderBHL;
import bio.guoda.preston.process.RegistryReaderBioCASE;
import bio.guoda.preston.process.RegistryReaderDOI;
import bio.guoda.preston.process.RegistryReaderDataONE;
import bio.guoda.preston.process.RegistryReaderGBIF;
import bio.guoda.preston.process.RegistryReaderIDigBio;
import bio.guoda.preston.process.RegistryReaderOBIS;
import bio.guoda.preston.process.RegistryReaderRSS;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.TikaHashingActivity;
import bio.guoda.preston.store.BlobStore;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.Quad;

import java.io.InputStream;
import java.util.Queue;
import java.util.stream.Stream;

/**
 * Command to (re-) process biodiversity dataset graph by providing some existing provenance logs.
 * <p>
 * Only considers already tracked datasets and their provenance.
 * <p>
 * See https://github.com/bio-guoda/preston/issues/15 .
 */

@Parameters(separators = "= ", commandDescription = "Generates Tika TLSH associated with any encountered hash://sha256 IRIs")
public class CmdTika extends CmdProcess {

    @Override
    protected Stream<StatementListener> createProcessors(BlobStore blobStore, StatementListener queueAsListener) {
        return Stream.of(
                new TikaHashingActivity(blobStore, queueAsListener)
        );
    }

}
