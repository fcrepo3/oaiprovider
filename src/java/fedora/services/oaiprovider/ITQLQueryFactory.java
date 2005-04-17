package fedora.services.oaiprovider;

import java.util.*;

import proai.error.*;

public class ITQLQueryFactory implements QueryFactory {

    private static final String QUERY_LANGUAGE = "itql";

    private String m_oaiItemID;
    
    public ITQLQueryFactory() {
    }

    public void init(Properties props) {
        m_oaiItemID = null;
    }

    public Map latestRecordDateQuery() {
        return null;
    }

/*

http://localhost:8080/fedora/risearch?
implied type=tuples
                                     &lang=iTQL|RDQL
implied format=Sparql
                                     &limit=[1 (default is no limit)]
                                     &distinct=[on (default is off)]
                                     &stream=[on (default is off)]
                                     &query=QUERY_TEXT_OR_URL

m_riSearch = "" + 
                     "?type=tuples" +
                     "&lang=iTQL" +
                     "&format=Sparql" +
                     "&query=QUERY_TEXT_OR_URL";
    }
    
        String latestDateQuery = 
            "select $date " +
            "from <#ri> " +
            "where " +
                "$object <info:fedora/fedora-system:def/view#disseminates> $diss " +
                "and $object <" + m_oaiItemID + "> $oaiItemID " +
                "and $diss <info:fedora/fedora-system:def/view#lastModifiedDate> $date " +
            "order by $date desc " +
            "limit 1;"; 
                     */
        
}
