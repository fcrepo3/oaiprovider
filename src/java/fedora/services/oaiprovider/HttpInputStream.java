package fedora.services.oaiprovider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;

/**
 * An InputStream from an HttpMethod.
 *
 * When this InputStream is close()d, the underlying http connection is
 * automatically released.
 */
public class HttpInputStream extends InputStream {

    private HttpClient m_client;
    private HttpMethod m_method;
    private String m_url;

    private int m_code;
    private InputStream m_in;

    public HttpInputStream(HttpClient client,
                           HttpMethod method,
                           String url) throws IOException {
        m_client = client;
        m_method = method;
        m_url = url;
        try {
            m_code = m_client.executeMethod(m_method);
            m_in = m_method.getResponseBodyAsStream();
            if (m_in == null) new ByteArrayInputStream(new byte[0]);
        } catch (IOException e) {
            m_method.releaseConnection();
            throw e;
        }
    }

    /**
     * Get the http method name (GET or POST).
     */
    public String getMethodName() {
        return m_method.getName();
    }

    /**
     * Get the original URL of the http request this InputStream is based on.
     */
    public String getURL() {
        return m_url;
    }

    /**
     * Get the http status code.
     */
    public int getStatusCode() {
        return m_code;
    }

    /**
     * Get the "reason phrase" associated with the status code.
     */
    public String getStatusText() {
        return m_method.getStatusLine().getReasonPhrase();
    }

    /**
     * Get a header value.
     */
    public Header getResponseHeader(String name) {
        return m_method.getResponseHeader(name);
    }

    /**
     * Automatically close on garbage collection.
     */
    public void finalize() {
        try { close(); } catch (Exception e) { }
    }

    //////////////////////////////////////////////////////////////////////////
    /////////////////// Methods from java.io.InputStream /////////////////////
    //////////////////////////////////////////////////////////////////////////

    public int read() throws IOException { return m_in.read(); }
    public int read(byte[] b) throws IOException { return m_in.read(b); }
    public int read(byte[] b, int off, int len) throws IOException { return m_in.read(b, off, len); }
    public long skip(long n) throws IOException { return m_in.skip(n); }
    public int available() throws IOException { return m_in.available(); }
    public void mark(int readlimit) { m_in.mark(readlimit); }
    public void reset() throws IOException { m_in.reset(); }
    public boolean markSupported() { return m_in.markSupported(); }

    /**
     * Release the underlying http connection and close the InputStream.
     */
    public void close() throws IOException {
        m_method.releaseConnection();
        m_in.close();
    }
}