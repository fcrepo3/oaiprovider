
package fedora.services.oaiprovider;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fedora.client.FedoraClient;
import fedora.common.PID;

public class InvocationSpec {

    private final String m_method;

    private final PID m_service;

    private final Map<String, String> m_params;

    private final boolean m_datastreamInvocation;

    private InvocationSpec(PID service,
                           String method,
                           Map<String, String> params,
                           boolean datastream) {
        m_method = method;
        m_service = service;
        m_params = params;
        m_datastreamInvocation = datastream;
    }

    public String method() {
        return m_method;
    }

    public PID service() {
        return m_service;
    }

    public String getDisseminationType(PID forWhom) {
        String object = (forWhom == null ? "*" : forWhom.toString());

        String service = "*";

        if (m_service != null) {
            service = m_service.toString();
        }

        if (isDatastreamInvocation()) {
            return "info:fedora/" + object + "/" + method();
        } else {
            String params = "";
            if (m_params.size() > 0) {
                params = getParamString();
            }

            return "info:fedora/" + object + "/" + service + "/" + method()
                    + params;
        }
    }

    public String getDisseminationType() {
        return getDisseminationType(null);
    }

    public boolean isDatastreamInvocation() {
        return m_datastreamInvocation;
    }

    public InputStream invoke(FedoraClient client, PID onWhom) {
        try {
            return client.get(getDisseminationType(onWhom), true);
        } catch (IOException e) {
            throw new RuntimeException("Could not disseminate content for "
                    + getDisseminationType(onWhom), e);
        }
    }

    /**
     * Build an InvocationSpec instance from the given properties.
     * <code>info:fedora/&#42;/{SDef|&#42;}/method[?param1=value1...]</code>
     */
    public static InvocationSpec getInstance(String disseminationType) {

        if (disseminationType == null || disseminationType.equals("")) {
            return null;
        }

        String spec = disseminationType.replace("info:fedora/", "");

        /* Simple obvious-mistake-filter for now */
        if (!spec.matches("[^\\/]+\\/[^\\/]+(\\/[^\\/]+){0,1}")) {
            throw new IllegalArgumentException("Unable to parse dissemination type "
                    + disseminationType);
        }

        String[] specParts = spec.split("/");

        PID service = null;
        String method = null;
        Map<String, String> params = new HashMap<String, String>();
        boolean datastream = false;

        if (specParts.length == 2) {
            /* This is a Datastrem dissemination */
            datastream = true;

            if (!specParts[0].equals("*")) {
                service = PID.getInstance(specParts[0]);
            }

            method = specParts[1];
        } else {
            /* This is a Service dissemination */

            if (!specParts[1].equals("*")) {
                service = PID.getInstance(specParts[1]);
            }

            String[] methodSpec = specParts[2].split("\\?");

            method = methodSpec[0];

            if (methodSpec.length == 2) {
                params = parseParams(methodSpec[1]);
            }

        }

        return new InvocationSpec(service, method, params, datastream);
    }

    private static Map<String, String> parseParams(String paramSpec) {
        Map<String, String> params = new HashMap<String, String>();

        for (String pair : paramSpec.split("&")) {
            String[] parts = pair.split("=");
            params.put(parts[0], parts[1]);
        }

        return params;
    }

    private String getParamString() {
        StringBuilder paramString = new StringBuilder("?");
        List<String> params = new ArrayList<String>(m_params.keySet());

        int paramSize = params.size();

        for (int i = 0; i < paramSize; i++) {
            String key = params.get(i);
            paramString.append(key);
            paramString.append("=");
            paramString.append(m_params.get(key));

            if (i < paramSize - 1) {
                paramString.append("&");
            }
        }

        return paramString.toString();
    }
}
