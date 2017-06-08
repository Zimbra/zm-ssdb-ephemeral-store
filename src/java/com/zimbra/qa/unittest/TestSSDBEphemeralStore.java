package com.zimbra.qa.unittest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.Assume;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.Pair;
import com.zimbra.cs.account.Config;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.account.soap.SoapProvisioning;
import com.zimbra.cs.ephemeral.EphemeralInput;
import com.zimbra.cs.ephemeral.EphemeralInput.RelativeExpiration;
import com.zimbra.cs.ephemeral.EphemeralKey;
import com.zimbra.cs.ephemeral.EphemeralLocation;
import com.zimbra.cs.ephemeral.EphemeralResult;
import com.zimbra.cs.ephemeral.EphemeralStore;
import com.zimbra.ssdb.SSDBEphemeralStore;

public class TestSSDBEphemeralStore extends TestCase {
    EphemeralStore store;
    private String SAMPLE_CSRF_TOKEN_DATA =  "69643d33363a30666532376439312d656339342d346534352d383436342d3339326262383736313364383b6578703d31333a313437333735383435373138323b7369643d31303a3131353031303934343a6b";
    private String SAMPLE_CSRF_TOKEN_DATA2 = "12356709873b29555213764d656339312d343934265d334352342d834362626333938373623313364383b6578703d313435373138323b7369643d31303a31313530333a3134373337353831303934343c72c";
    private String SAMPLE_CSRF_TOKEN_DATA3 = "098123567552173b29535633764d69312d426534393d3342d83435232633343626932331383736364383b6578703d3134353731369643d8323b7331303530333a31313a333373134375303943831372bc343";
    private String SAMPLE_CSRF_TOKEN_CRUMB =  "3822663c52f27487f172055ddc0918aa";
    private String SAMPLE_CSRF_TOKEN_CRUMB2 = "2931453c52fb2487a172095ddc4908ac";
    private String SAMPLE_CSRF_TOKEN_CRUMB3 = "2c52f931453b248095dd7a172c49c08a";
    private String SAMPLE_AUTH_TOKEN =  "366778080";
    private String SAMPLE_AUTH_TOKEN2 = "456779043";
    private String SAMPLE_AUTH_TOKEN3 = "437745690";
    private String SAMPLE_AUTH_TOKEN_VERSION = "8.7.0_GA_1659";
    private String ACCOUNT_ID = "47e456be-b00a-465e-a1db-4b53e64fa";
    private boolean SSDBStoreConfigured = false;
    private List<Pair<EphemeralInput, EphemeralLocation>> toDelete = new ArrayList<Pair<EphemeralInput, EphemeralLocation>>();

    @Override
    public void setUp() throws Exception {
        String ssdbUrl = Provisioning.getInstance().getConfig().getEphemeralBackendURL();
        String toks[] = ssdbUrl.split(":");
        if(toks != null && toks.length == 3 && "ssdb".equalsIgnoreCase(toks[0])) {
            EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
            EphemeralStore.Factory factory = EphemeralStore.getFactory();
            factory.test(ssdbUrl);
            store = factory.getStore();
            assertTrue(store instanceof SSDBEphemeralStore);
            SSDBStoreConfigured = true;
        } else {
            SSDBStoreConfigured = false;
        }
    }

    @Override
    public void tearDown() throws Exception {
        if (SSDBStoreConfigured) {
            for (Pair<EphemeralInput, EphemeralLocation> pair: toDelete) {
                EphemeralInput input = pair.getFirst();
                EphemeralLocation location = pair.getSecond();
                try {
                    store.delete(input.getEphemeralKey(), (String) input.getValue(), location);
                } catch (ServiceException e) {
                    // ignore failed deletions
                }
            }
            toDelete.clear();
            SSDBEphemeralStore.getFactory().shutdown();
        }
    }

    private void addDeletionEntry(EphemeralInput input, EphemeralLocation location) {
        toDelete.add(new Pair<EphemeralInput, EphemeralLocation>(input, location));
    }

    private void doSet(EphemeralInput input, EphemeralLocation location) throws ServiceException {
        store.set(input, location);
        addDeletionEntry(input, location);
    }

    private void doUpdate(EphemeralInput input, EphemeralLocation location) throws ServiceException {
        store.update(input, location);
        addDeletionEntry(input, location);
    }

    @Test
    public void testSetGetLastLogin() throws Exception {
        Assume.assumeTrue(SSDBStoreConfigured);
        String firstLogin = "20160912212057.178Z";
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", ACCOUNT_ID }; }
        };
        EphemeralKey eKey = new EphemeralKey(Provisioning.A_zimbraLastLogonTimestamp);
        EphemeralInput attr = new EphemeralInput(eKey, firstLogin);
        doSet(attr, accountIDLocation);
        EphemeralResult retAttr = store.get(eKey, accountIDLocation);
        assertNotNull(retAttr);
        assertEquals("Found incorrect last logon timestamp value",firstLogin, retAttr.getValue());
    }

    @Test
    public void testSetGetOverwriteLastLogin() throws Exception {
        Assume.assumeTrue(SSDBStoreConfigured);
        String firstLogin = "20160912212057.178Z";
        String lastLogin = "20160912220045.178Z";
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", ACCOUNT_ID }; }
        };
        EphemeralKey eKey = new EphemeralKey(Provisioning.A_zimbraLastLogonTimestamp);
        EphemeralInput attr = new EphemeralInput(eKey, firstLogin);
        EphemeralInput attrLatest = new EphemeralInput(eKey, lastLogin);
        doSet(attr, accountIDLocation);
        doSet(attrLatest, accountIDLocation);
        EphemeralResult retAttr = store.get(eKey, accountIDLocation);
        assertNotNull(retAttr);
        assertEquals("Found incorrect last logon timestamp value",lastLogin, retAttr.getValue());
    }

    @Test
    public void testSetUpdateLastLogin() throws Exception {
        Assume.assumeTrue(SSDBStoreConfigured);
        String firstLogin = "20160912212057.178Z";
        String lastLogin = "20160912220045.178Z";
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", ACCOUNT_ID }; }
        };
        EphemeralKey eKey = new EphemeralKey(Provisioning.A_zimbraLastLogonTimestamp);
        EphemeralInput attr = new EphemeralInput(eKey, firstLogin);
        EphemeralInput attrLatest = new EphemeralInput(eKey, lastLogin);
        doSet(attr, accountIDLocation);
        doUpdate(attrLatest, accountIDLocation);
        EphemeralResult retAttr = store.get(eKey, accountIDLocation);
        assertNotNull(retAttr);
        assertEquals("Found incorrect last logon timestamp value", lastLogin, retAttr.getValue());
    }

    @Test
    public void testHasValidAuthToken() throws Exception {
        Assume.assumeTrue(SSDBStoreConfigured);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", ACCOUNT_ID }; }
        };
        EphemeralKey eKey = new EphemeralKey(Provisioning.A_zimbraAuthTokens, SAMPLE_AUTH_TOKEN);
        EphemeralInput attr = new EphemeralInput(eKey, SAMPLE_AUTH_TOKEN_VERSION);
        doSet(attr, accountIDLocation);
        assertTrue("Should find this auth token", store.has(eKey, accountIDLocation));
    }

    @Test
    public void testHasInvalidAuthToken() throws Exception {
        Assume.assumeTrue(SSDBStoreConfigured);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", ACCOUNT_ID }; }
        };
        EphemeralKey eKey = new EphemeralKey(Provisioning.A_zimbraAuthTokens, SAMPLE_AUTH_TOKEN);
        EphemeralKey eKey2 = new EphemeralKey(Provisioning.A_zimbraAuthTokens, SAMPLE_AUTH_TOKEN2);
        EphemeralInput attr = new EphemeralInput(eKey, SAMPLE_AUTH_TOKEN_VERSION);
        doSet(attr, accountIDLocation);
        assertFalse("should not find this auth token ", store.has(eKey2, accountIDLocation));
    }


    @Test
    public void testHasValidCsrfToken() throws Exception {
        Assume.assumeTrue(SSDBStoreConfigured);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account",  ACCOUNT_ID}; }
        };
        EphemeralKey eKey = new EphemeralKey(Provisioning.A_zimbraCsrfTokenData, SAMPLE_CSRF_TOKEN_CRUMB);
        EphemeralInput attr = new EphemeralInput(eKey, SAMPLE_CSRF_TOKEN_DATA);
        doSet(attr, accountIDLocation);
        assertTrue("Should find this CSRF token crumb", store.has(eKey, accountIDLocation));
    }

    @Test
    public void testHasInvalidCsrfToken() throws Exception {
        Assume.assumeTrue(SSDBStoreConfigured);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account",  ACCOUNT_ID}; }
        };
        EphemeralKey eKey1 = new EphemeralKey(Provisioning.A_zimbraCsrfTokenData, SAMPLE_CSRF_TOKEN_CRUMB);
        EphemeralKey eKey2 = new EphemeralKey(Provisioning.A_zimbraCsrfTokenData, SAMPLE_CSRF_TOKEN_CRUMB2);
        EphemeralInput attr = new EphemeralInput(eKey1, SAMPLE_CSRF_TOKEN_DATA);
        doSet(attr, accountIDLocation);
        assertFalse("should not find this CSRF token crumb ", store.has(eKey2, accountIDLocation));
    }

    @Test
    public void testUpdateCsrfToken() throws Exception {
        Assume.assumeTrue(SSDBStoreConfigured);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account",  ACCOUNT_ID}; }
        };
        EphemeralKey eKey = new EphemeralKey(Provisioning.A_zimbraCsrfTokenData, SAMPLE_CSRF_TOKEN_CRUMB);
        EphemeralInput attr = new EphemeralInput(eKey, SAMPLE_CSRF_TOKEN_DATA);
        EphemeralInput updatedAttr = new EphemeralInput(eKey, SAMPLE_CSRF_TOKEN_DATA2);
        doSet(attr, accountIDLocation);
        doSet(updatedAttr, accountIDLocation);
        assertTrue("Should find the CSRF token crumb that was just saved", store.has(eKey, accountIDLocation));
        assertEquals(SAMPLE_CSRF_TOKEN_DATA2, store.get(eKey, accountIDLocation).getValue());
    }

    @Test
    public void testMultipleCsrfTokens() throws Exception {
        Assume.assumeTrue(SSDBStoreConfigured);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account",  ACCOUNT_ID}; }
        };
        EphemeralKey eKey1 = new EphemeralKey(Provisioning.A_zimbraCsrfTokenData, SAMPLE_CSRF_TOKEN_CRUMB);
        EphemeralKey eKey2 = new EphemeralKey(Provisioning.A_zimbraCsrfTokenData, SAMPLE_CSRF_TOKEN_CRUMB2);
        EphemeralKey eKey3 = new EphemeralKey(Provisioning.A_zimbraCsrfTokenData, SAMPLE_CSRF_TOKEN_CRUMB3);

        EphemeralInput attrVal1 = new EphemeralInput(eKey1, SAMPLE_CSRF_TOKEN_DATA);
        EphemeralInput attrVal2 = new EphemeralInput(eKey2, SAMPLE_CSRF_TOKEN_DATA2);
        EphemeralInput attrVal3 = new EphemeralInput(eKey3, SAMPLE_CSRF_TOKEN_DATA3);

        doSet(attrVal1, accountIDLocation);
        doSet(attrVal2, accountIDLocation);
        doSet(attrVal3, accountIDLocation);

        assertTrue(store.has(eKey1, accountIDLocation));
        assertTrue(store.has(eKey2, accountIDLocation));
        assertTrue(store.has(eKey3, accountIDLocation));

        assertEquals(SAMPLE_CSRF_TOKEN_DATA, store.get(eKey1, accountIDLocation).getValue());
        assertEquals(SAMPLE_CSRF_TOKEN_DATA2, store.get(eKey2, accountIDLocation).getValue());
        assertEquals(SAMPLE_CSRF_TOKEN_DATA3, store.get(eKey3, accountIDLocation).getValue());
    }

    @Test
    public void testMultipleAuthTokens() throws Exception {
        Assume.assumeTrue(SSDBStoreConfigured);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account",  ACCOUNT_ID}; }
        };
        EphemeralKey eKey1 = new EphemeralKey(Provisioning.A_zimbraAuthTokens, SAMPLE_AUTH_TOKEN);
        EphemeralKey eKey2 = new EphemeralKey(Provisioning.A_zimbraAuthTokens, SAMPLE_AUTH_TOKEN2);
        EphemeralKey eKey3 = new EphemeralKey(Provisioning.A_zimbraAuthTokens, SAMPLE_AUTH_TOKEN3);

        EphemeralInput attrVal1 = new EphemeralInput(eKey1, SAMPLE_AUTH_TOKEN_VERSION);
        EphemeralInput attrVal2 = new EphemeralInput(eKey2, SAMPLE_AUTH_TOKEN_VERSION);
        EphemeralInput attrVal3 = new EphemeralInput(eKey3, SAMPLE_AUTH_TOKEN_VERSION);

        doSet(attrVal1, accountIDLocation);
        doSet(attrVal2, accountIDLocation);
        doSet(attrVal3, accountIDLocation);

        assertTrue(store.has(eKey1, accountIDLocation));
        assertTrue(store.has(eKey2, accountIDLocation));
        assertTrue(store.has(eKey3, accountIDLocation));

        assertEquals(SAMPLE_AUTH_TOKEN_VERSION, store.get(eKey1, accountIDLocation).getValue());
        assertEquals(SAMPLE_AUTH_TOKEN_VERSION, store.get(eKey2, accountIDLocation).getValue());
        assertEquals(SAMPLE_AUTH_TOKEN_VERSION, store.get(eKey3, accountIDLocation).getValue());
    }

    @Test
    public void testAuthTokenExpiration() throws Exception {
        Assume.assumeTrue(SSDBStoreConfigured);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account",  ACCOUNT_ID}; }
        };
        EphemeralKey eKey1 = new EphemeralKey(Provisioning.A_zimbraAuthTokens, SAMPLE_AUTH_TOKEN);
        RelativeExpiration exp = new RelativeExpiration(1000L, TimeUnit.MILLISECONDS);
        EphemeralInput attrVal1 = new EphemeralInput(eKey1, SAMPLE_AUTH_TOKEN_VERSION, exp);
        doSet(attrVal1, accountIDLocation);
        assertTrue("Token should be in SSDB before it expires", store.has(eKey1, accountIDLocation));
        Thread.sleep(2000);
        assertFalse("Token should be gone after 2 seconds", store.has(eKey1, accountIDLocation));
    }

    @Test
    public void testDeleteAuthToken() throws Exception {
        Assume.assumeTrue(SSDBStoreConfigured);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account",  ACCOUNT_ID}; }
        };
        EphemeralKey eKey1 = new EphemeralKey(Provisioning.A_zimbraAuthTokens, SAMPLE_AUTH_TOKEN);
        RelativeExpiration exp = new RelativeExpiration(600000L, TimeUnit.MILLISECONDS);
        EphemeralInput attrVal1 = new EphemeralInput(eKey1, SAMPLE_AUTH_TOKEN_VERSION, exp);
        doSet(attrVal1, accountIDLocation);
        assertTrue("Token should be in SSDB after insertion", store.has(eKey1, accountIDLocation));
        store.delete(eKey1, SAMPLE_AUTH_TOKEN_VERSION, accountIDLocation);
        assertFalse("Token should be gone from SSDB after deletion", store.has(eKey1, accountIDLocation));
    }

    @Test
    public void testDeleteLastLogon() throws Exception {
        Assume.assumeTrue(SSDBStoreConfigured);
        String lastLogon = "20160912212057.178Z";
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account",  ACCOUNT_ID}; }
        };
        EphemeralKey eKey = new EphemeralKey(Provisioning.A_zimbraLastLogonTimestamp);
        EphemeralInput attr = new EphemeralInput(eKey, lastLogon);
        doSet(attr, accountIDLocation);
        assertTrue("Last logon value should be present before deletion", store.has(eKey, accountIDLocation));
        store.delete(eKey, lastLogon, accountIDLocation);
        assertFalse("Last logon value should be gone after deletion", store.has(eKey, accountIDLocation));
    }

    @Test
    public void testBadUrl() throws ServiceException {
        String[] badUrls = new String[]{"ssdb:badhost", "ssdb:badhost:badport"};
        Config conf = SoapProvisioning.getInstance().getConfig();
        for (String badUrl: badUrls) {
            try {
                conf.setEphemeralBackendURL(badUrl);
                fail(String.format("should not be able to set backend URL to %s", badUrl));
            } catch (ServiceException e) {}
        }
    }
}
