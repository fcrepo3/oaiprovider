
package fedora.services.oaiprovider;

import proai.MetadataFormat;

/**
 * @author Edwin Shin
 */
public class FedoraMetadataFormat
        implements MetadataFormat {

    private final String m_prefix;

    private final String m_namespaceURI;

    private final String m_schemaLocation;

    private final InvocationSpec m_mdSpec;

    private final InvocationSpec m_aboutSpec;

    public FedoraMetadataFormat(String prefix,
                                String namespaceURI,
                                String schemaLocation,
                                InvocationSpec mdSpec,
                                InvocationSpec aboutSpec) {
        m_prefix = prefix;
        m_namespaceURI = namespaceURI;
        m_schemaLocation = schemaLocation;
        m_mdSpec = mdSpec;
        m_aboutSpec = aboutSpec;
    }

    public String getPrefix() {
        return m_prefix;
    }

    public String getNamespaceURI() {
        return m_namespaceURI;
    }

    public String getSchemaLocation() {
        return m_schemaLocation;
    }

    public InvocationSpec getMetadataSpec() {
        return m_mdSpec;
    }

    public InvocationSpec getAboutSpec() {
        return m_aboutSpec;
    }
}
