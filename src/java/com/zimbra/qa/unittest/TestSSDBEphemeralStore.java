package com.zimbra.qa.unittest;

import static org.junit.Assert.*;
import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.ephemeral.EphemeralInput;
import com.zimbra.cs.ephemeral.EphemeralLocation;
import com.zimbra.cs.ephemeral.EphemeralResult;
import com.zimbra.cs.ephemeral.EphemeralStore;
import com.zimbra.ssdb.SSDBEphemeralStore;

public class TestSSDBEphemeralStore extends TestCase {
    EphemeralStore store;
    private String SAMPLE_CSRF_TOKEN = "69643d33363a30666532376439312d656339342d346534352d383436342d3339326262383736313364383b6578703d31333a313437333735383435373138323b7369643d31303a313135303130393434363b:3822663c52f27487f172055ddc0918aa:1473758457182";
    private String SAMPLE_AUTH_TOKEN = "366778080|1473761137744|8.7.0_GA_1659";
    private String SAMPLE_AUTH_TOKEN2 = "1929781990|1473758457182|8.7.0_GA_1659";
    @Override
    public void setUp() throws Exception {
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        store = SSDBEphemeralStore.getFactory().getStore();
        SSDBEphemeralStore.getFactory().startup();
    }

    @Override
    public void tearDown() throws Exception {
        SSDBEphemeralStore.getFactory().shutdown();
    }

    @Test    
    public void testSetGetLastLogin() throws Exception {
        String firstLogin = "20160912212057.178Z";
        EphemeralStore store = SSDBEphemeralStore.getFactory().getStore();
        assertTrue(store instanceof SSDBEphemeralStore);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        EphemeralInput attr = new EphemeralInput(Provisioning.A_zimbraLastLogonTimestamp, firstLogin);
        store.set(attr, accountIDLocation);
        
        EphemeralResult retAttr = store.get(Provisioning.A_zimbraLastLogonTimestamp, accountIDLocation);
        assertNotNull(retAttr);
        assertEquals(firstLogin, retAttr.getValue());
    }

    @Test    
    public void testSetGetOverwriteLastLogin() throws Exception {
        String firstLogin = "20160912212057.178Z";
        String lastLogin = "20160912220045.178Z";
        EphemeralStore store = SSDBEphemeralStore.getFactory().getStore();
        assertTrue(store instanceof SSDBEphemeralStore);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        EphemeralInput attr = new EphemeralInput(Provisioning.A_zimbraLastLogonTimestamp, firstLogin);
        EphemeralInput attrLatest = new EphemeralInput(Provisioning.A_zimbraLastLogonTimestamp, lastLogin);
        store.set(attr, accountIDLocation);
        store.set(attrLatest, accountIDLocation);
        EphemeralResult retAttr = store.get(Provisioning.A_zimbraLastLogonTimestamp, accountIDLocation);
        assertNotNull(retAttr);
        assertEquals(lastLogin, retAttr.getValue());
    }
    
    @Test    
    public void testSetUpdateLastLogin() throws Exception {
        String firstLogin = "20160912212057.178Z";
        String lastLogin = "20160912220045.178Z";
        EphemeralStore store = SSDBEphemeralStore.getFactory().getStore();
        assertTrue(store instanceof SSDBEphemeralStore);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        EphemeralInput attr = new EphemeralInput(Provisioning.A_zimbraLastLogonTimestamp, firstLogin);
        EphemeralInput attrLatest = new EphemeralInput(Provisioning.A_zimbraLastLogonTimestamp, lastLogin);
        store.set(attr, accountIDLocation);
        store.update(attrLatest, accountIDLocation);
        EphemeralResult retAttr = store.get(Provisioning.A_zimbraLastLogonTimestamp, accountIDLocation);
        assertNotNull(retAttr);
        assertEquals(lastLogin, retAttr.getValue());
    }
    
    @Test
    public void testHasAuthToken() throws Exception {
        EphemeralStore store = SSDBEphemeralStore.getFactory().getStore();
        assertTrue(store instanceof SSDBEphemeralStore);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        EphemeralInput attr = new EphemeralInput(Provisioning.A_zimbraAuthTokens, SAMPLE_AUTH_TOKEN);
        store.set(attr, accountIDLocation);
        assertTrue(store.has(Provisioning.A_zimbraAuthTokens, SAMPLE_AUTH_TOKEN, accountIDLocation));
    }
}
