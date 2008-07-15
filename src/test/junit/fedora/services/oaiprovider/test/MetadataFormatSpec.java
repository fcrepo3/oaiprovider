
package fedora.services.oaiprovider.test;

public class MetadataFormatSpec {

    private final String m_prefix;

    private final DisseminationOrigin m_mo;

    private final DisseminationOrigin m_ao;

    public MetadataFormatSpec(String prefix,
                              DisseminationOrigin metadataOrigin,
                              DisseminationOrigin aboutsOrigin) {
        m_prefix = prefix;
        m_mo = metadataOrigin;
        m_ao = aboutsOrigin;
    }

    public String getPrefix() {
        return m_prefix;
    }

    public DisseminationOrigin getMetadataOrigin() {
        return m_mo;
    }

    public DisseminationOrigin getAboutsOrigin() {
        return m_ao;
    }
}
