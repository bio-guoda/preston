package bio.guoda.preston.server;

import bio.guoda.preston.cmd.LoggingPersisting;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import picocli.CommandLine;

import javax.servlet.Servlet;
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

    @CommandLine.Option(
            names = {"-m", "--mode"},
            description = "mode of operation. Supported values: ${COMPLETION-CANDIDATES}.\""
    )
    ServerModes mode = ServerModes.content;

    @Override
    public void run() {
        Server server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        connector.setHost(host);
        server.setConnectors(new Connector[] {connector});
        ServletHandler servletHandler = new ServletHandler();
        ServletHolder servletHolder = new ServletHolder(getServletClass());
        Map<String, String> properties = new TreeMap<String, String>() {{
            put(PropertyNames.PRESTON_PROPERTY_LOCAL_PATH, getLocalDataDir());
            put(PropertyNames.PRESTON_PROPERTY_REMOTE_PATH, StringUtils.join(getRemotes(), ","));
            put(PropertyNames.PRESTON_PROPERTY_CACHE_ENABLED, Boolean.toString(isCacheEnabled()));
            put(PropertyNames.PRESTON_PROPERTY_TMP_PATH, getLocalTmpDir());
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

    private Class<? extends Servlet> getServletClass() {
        switch (mode) {
            case registry:
                return RegistryServlet.class;
            default:
            case content:
                return ContentServlet.class;
        }
    }

}

