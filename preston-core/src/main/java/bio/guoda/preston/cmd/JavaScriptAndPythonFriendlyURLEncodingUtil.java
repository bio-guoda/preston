package bio.guoda.preston.cmd;

import org.apache.commons.lang3.StringUtils;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public final class JavaScriptAndPythonFriendlyURLEncodingUtil {
    public static String urlEncode(String str) {
        try {
            ScriptEngineManager manager = new ScriptEngineManager();
            ScriptEngine engine = manager.getEngineByName("JavaScript");
            String quoteEscaped = StringUtils.replace(str, "'", "&quot;");
            return  (String) engine.eval("encodeURIComponent('" + quoteEscaped + "')");
        } catch (ScriptException e) {
            return str;
        }

    }
}
