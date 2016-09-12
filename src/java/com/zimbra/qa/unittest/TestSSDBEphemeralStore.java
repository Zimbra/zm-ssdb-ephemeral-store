package com.zimbra.qa.unittest;

import static org.junit.Assert.*;
import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zimbra.cs.ephemeral.EphemeralInput;
import com.zimbra.cs.ephemeral.EphemeralLocation;
import com.zimbra.cs.ephemeral.EphemeralResult;
import com.zimbra.cs.ephemeral.EphemeralStore;
import com.zimbra.ssdb.SSDBEphemeralStore;

public class TestSSDBEphemeralStore extends TestCase {
    EphemeralStore store;
    private String SAMPLE_CSRF_TOKEN = "69643d33363a65323334333662662d363963622d346438342d626235342d3634653934386563353835303b6578703d31333a313437323135383737383835363b7369643d31303a2d3635383236353630383b:47bfdd7bc4a1040ddb7646e7b0faf4ea:1472158778856";
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

    public void testSetGetCsrf() throws Exception {
        EphemeralStore store = SSDBEphemeralStore.getFactory().getStore();
        assertTrue(store instanceof SSDBEphemeralStore);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {

            @Override
            public String[] getLocation() { return new String[] { "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        EphemeralInput attr = new EphemeralInput(SAMPLE_CSRF_TOKEN, true);
        store.set(attr, accountIDLocation);
        
        EphemeralResult retAttr = store.get(SAMPLE_CSRF_TOKEN, accountIDLocation);
        assertNotNull(retAttr);
        assertTrue(retAttr.getBoolValue());
    }

}
