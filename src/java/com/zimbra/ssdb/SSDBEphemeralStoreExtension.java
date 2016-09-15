package com.zimbra.ssdb;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.extension.ExtensionException;
import com.zimbra.cs.extension.ZimbraExtension;
import com.zimbra.qa.unittest.TestSSDBEphemeralStore;
import com.zimbra.qa.unittest.ZimbraSuite;

/**
 * 
 * @author Greg Solovyev
 *
 */
public class SSDBEphemeralStoreExtension implements ZimbraExtension {
    public static final String EXTENSION_NAME = "com_zimbra_ssdb_ephemeral_store";
    @Override
    public String getName() {
        return EXTENSION_NAME;
    }

    @Override
    public void init() throws ExtensionException, ServiceException {
        try {
            ZimbraSuite.addTest(TestSSDBEphemeralStore.class);
        } catch (NoClassDefFoundError e) {
            // Expected in production, because JUnit is not available.
            ZimbraLog.test.debug("Unable to load TestSearchResultsDownload unit tests.", e);
        }
    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub

    }

}
