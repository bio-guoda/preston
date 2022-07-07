package bio.guoda.preston.server;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.cmd.LogErrorHandlerExitOnError;
import bio.guoda.preston.cmd.LoggingPersisting;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import org.apache.commons.io.output.NullPrintStream;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import picocli.CommandLine;

import java.util.Map;
import java.util.TreeMap;

@CommandLine.Command(
        name = "serve",
        aliases = {"s", "server"},
        description = "provide access to content via http endpoint"
)
public class CmdServe extends LoggingPersisting implements Runnable {

    @CommandLine.Option(
            names = {"-p", "--port"},
            description = "port to listen on"
    )
    Integer port = 8080;

    @CommandLine.Option(
            names = {"-h", "--host"},
            description = "host to listen on"
    )
    String host = "localhost";


    @Override
    public void run() {
        Server server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        connector.setHost(host);
        server.setConnectors(new Connector[] {connector});
        ServletHandler servletHandler = new ServletHandler();
        ServletHolder servletHolder = new ServletHolder(ContentServlet.class);
        Map<String, String> properties = new TreeMap<String, String>() {{
            put(PropertyNames.PRESTON_PROPERTY_LOCAL_PATH, getLocalDataDir());
            put(PropertyNames.PRESTON_PROPERTY_REMOTE_PATH, StringUtils.join(getRemotes(), ","));
            put(PropertyNames.PRESTON_PROPERTY_CACHE_ENABLED, Boolean.toString(isCacheEnabled()));
        }};

        servletHolder.setInitParameters(properties);

        servletHandler.addServletWithMapping(servletHolder, "/");
        server.insertHandler(servletHandler);
        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

