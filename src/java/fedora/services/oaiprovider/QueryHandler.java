package fedora.services.oaiprovider;

import java.util.Date;
import proai.error.RepositoryException;

/**
 * @author Edwin Shin
 */
public interface QueryHandler {
    public Date getLatestDate() throws RepositoryException;
}
