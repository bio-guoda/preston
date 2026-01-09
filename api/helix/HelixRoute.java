// Etherion-Proxy-v1 /api/helix endpoint
// Minimal, read-only, auth-free
// Follow-up to deleted Issue #363

package bio.guoda.preston.api;

import spark.Request;
import spark.Response;
import spark.Route;

public class HelixRoute implements Route {
    @Override
    public Object handle(Request req, Response res) throws Exception {
        res.type("application/json");
        return "{\"status\":\"ok\",\"service\":\"helix\",\"version\":\"1\"}";
    }
}
