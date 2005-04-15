package fedora.services.oaiprovider;

import proai.MetadataFormat;

/**
 * @author Edwin Shin
 */
public class FedoraMetadataFormat implements MetadataFormat {
    private String m_prefix;
    private String m_namespaceURI;
    private String m_schemaLocation;
    private String m_dissType;

    public FedoraMetadataFormat(String prefix,
                              String namespaceURI,
                              String schemaLocation, 
                              String dissType) {
        m_prefix = prefix;
        m_namespaceURI = namespaceURI;
        m_schemaLocation = schemaLocation;
        m_dissType = dissType;
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
    
    public String getDissType() {
        return m_dissType;
    }
}
