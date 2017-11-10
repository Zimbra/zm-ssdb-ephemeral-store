package com.zimbra.ssdb;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.ephemeral.EphemeralInput;
import com.zimbra.cs.ephemeral.EphemeralInput.AbsoluteExpiration;
import com.zimbra.cs.ephemeral.EphemeralInput.Expiration;
import com.zimbra.cs.ephemeral.EphemeralKey;
import com.zimbra.cs.ephemeral.EphemeralLocation;
import com.zimbra.cs.ephemeral.EphemeralStore;
import com.zimbra.cs.mailbox.MailboxTestUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class SSDBEphemeralStoreTest {

    @Mock
    private JedisPool mockJedisPool;

    @Mock
    private Jedis jedis;

    @Before
    public void setUp() throws Exception {
        jedis = EasyMock.mock(Jedis.class);
        mockJedisPool = EasyMock.mock(JedisPool.class);
        MailboxTestUtil.initServer("../zm-mailbox/store/");
        Provisioning.getInstance().getConfig().setEphemeralBackendURL("ssdb:localhost:8888");
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testFactory() throws ServiceException {
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        EphemeralStore store = SSDBEphemeralStore.getFactory().getStore();
        assertTrue(store instanceof SSDBEphemeralStore);
    }

    @Test
    public void testShutdown() throws ServiceException {
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        EphemeralStore store = SSDBEphemeralStore.getFactory().getStore();
        assertTrue(store instanceof SSDBEphemeralStore);

        ((SSDBEphemeralStore)store).setPool(mockJedisPool);
        mockJedisPool.close();
        mockJedisPool.destroy();
        expectLastCall().once();
        replay(mockJedisPool);
        SSDBEphemeralStore.getFactory().shutdown();
        verify(mockJedisPool);
    }

    @Test
    public void testGet() throws ServiceException {
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        EphemeralStore store = SSDBEphemeralStore.getFactory().getStore();
        assertTrue(store instanceof SSDBEphemeralStore);

        ((SSDBEphemeralStore)store).setPool(mockJedisPool);
        EphemeralLocation cosLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "cos", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        expect(mockJedisPool.getResource()).andReturn(jedis).atLeastOnce();
        expect(jedis.ping()).andReturn(null);
        expect(jedis.get("cos|47e456be-b00a-465e-a1db-4b53e64fa|somekey")).andReturn(null);
        jedis.close();
        replay(mockJedisPool);
        replay(jedis);
        EphemeralKey eKey = new EphemeralKey("somekey");
        store.get(eKey, cosLocation);
        verify(mockJedisPool);
        verify(jedis);
    }

    @Test
    public void testDelete() throws ServiceException {
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        EphemeralStore store = SSDBEphemeralStore.getFactory().getStore();
        assertTrue(store instanceof SSDBEphemeralStore);

        ((SSDBEphemeralStore)store).setPool(mockJedisPool);
        EphemeralLocation cosLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "cos", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        expect(mockJedisPool.getResource()).andReturn(jedis).atLeastOnce();
        expect(jedis.ping()).andReturn(null);
        expect(jedis.del("cos|47e456be-b00a-465e-a1db-4b53e64fa|someattr")).andReturn(null);
        jedis.close();
        replay(mockJedisPool);
        replay(jedis);
        EphemeralKey eKey = new EphemeralKey("someattr");
        store.delete(eKey, "value", cosLocation);
        verify(mockJedisPool);
        verify(jedis);
    }

    @Test
    public void testSetDynamic() throws ServiceException {
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        EphemeralStore store = SSDBEphemeralStore.getFactory().getStore();
        assertTrue(store instanceof SSDBEphemeralStore);

        EphemeralKey eKey = new EphemeralKey("testK", "testD");
        EphemeralInput kv = new EphemeralInput(eKey,"testV");
        ((SSDBEphemeralStore)store).setPool(mockJedisPool);
        EphemeralLocation domainLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "domain", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        expect(mockJedisPool.getResource()).andReturn(jedis).atLeastOnce();
        expect(jedis.ping()).andReturn(null);
        expect(jedis.set("domain|47e456be-b00a-465e-a1db-4b53e64fa|testK|testD","testV|")).andReturn("testK");
        jedis.close();
        replay(mockJedisPool);
        replay(jedis);
        store.set(kv, domainLocation);
        verify(mockJedisPool);
        verify(jedis);
    }

    @Test
    public void testSetWithTTL() throws ServiceException {
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        EphemeralStore store = SSDBEphemeralStore.getFactory().getStore();
        assertTrue(store instanceof SSDBEphemeralStore);

        EphemeralKey eKey = new EphemeralKey("testK", "testD");
        Long millis = System.currentTimeMillis();
        Expiration exp = new MockAbsoluteExpiration(millis + 2000L);
        int ttl = (int)(exp.getRelativeMillis()/1000);
        EphemeralInput kv = new EphemeralInput(eKey,"testV", exp);
        ((SSDBEphemeralStore)store).setPool(mockJedisPool);
        EphemeralLocation domainLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "domain", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        expect(mockJedisPool.getResource()).andReturn(jedis).atLeastOnce();
        String val = String.format("testV|%s", exp.getMillis());
        expect(jedis.ping()).andReturn(null);
        expect(jedis.setex("domain|47e456be-b00a-465e-a1db-4b53e64fa|testK|testD",ttl,val)).andReturn("testK");
        jedis.close();
        replay(mockJedisPool);
        replay(jedis);
        store.set(kv, domainLocation);
        verify(mockJedisPool);
        verify(jedis);
    }

    @Test
    public void testSetNonDynamic() throws ServiceException {
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        EphemeralStore store = SSDBEphemeralStore.getFactory().getStore();
        assertTrue(store instanceof SSDBEphemeralStore);

        EphemeralKey eKey = new EphemeralKey("testK");
        EphemeralInput kv = new EphemeralInput(eKey,"testV");
        ((SSDBEphemeralStore)store).setPool(mockJedisPool);
        EphemeralLocation domainLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "domain", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        expect(mockJedisPool.getResource()).andReturn(jedis).atLeastOnce();
        expect(jedis.ping()).andReturn(null);
        expect(jedis.set("domain|47e456be-b00a-465e-a1db-4b53e64fa|testK","testV|")).andReturn("testK");
        jedis.close();
        replay(mockJedisPool);
        replay(jedis);
        store.set(kv, domainLocation);
        verify(mockJedisPool);
        verify(jedis);
    }

    @Test
    public void testLastLogonTimestampToKey() throws ServiceException {
        String lastLogonTime = "20160912212057.178Z";
        EphemeralKey eKey = new EphemeralKey(Provisioning.A_zimbraLastLogonTimestamp);
        EphemeralInput input = new EphemeralInput(eKey, lastLogonTime);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        SSDBEphemeralStore store = (SSDBEphemeralStore)SSDBEphemeralStore.getFactory().getStore();

        assertEquals("account|47e456be-b00a-465e-a1db-4b53e64fa|zimbraLastLogonTimestamp", store.toKey(input, accountIDLocation));
    }

    @Test
    public void testLastLogonTimestampToValue() throws ServiceException {
        String lastLogonTime = "20160912212057.178Z";
        EphemeralKey eKey = new EphemeralKey(Provisioning.A_zimbraLastLogonTimestamp);
        EphemeralInput input = new EphemeralInput(eKey, lastLogonTime);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        SSDBEphemeralStore store = (SSDBEphemeralStore)SSDBEphemeralStore.getFactory().getStore();
        assertEquals(String.format("%s|", lastLogonTime), store.toValue(input, accountIDLocation));
    }

    @Test
    public void testAuthTokenToKey() throws ServiceException {
        Expiration exp = new AbsoluteExpiration(1473761137744L);
        EphemeralKey eKey = new EphemeralKey(Provisioning.A_zimbraAuthTokens, "366778080");
        EphemeralInput input = new EphemeralInput(eKey, "8.7.0_GA_1659", exp);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        SSDBEphemeralStore store = (SSDBEphemeralStore)SSDBEphemeralStore.getFactory().getStore();
        assertEquals("account|47e456be-b00a-465e-a1db-4b53e64fa|zimbraAuthTokens|366778080", store.toKey(input, accountIDLocation));
    }

    @Test
    public void testAuthTokenToValue() throws ServiceException {
        Expiration exp = new AbsoluteExpiration(1473761137744L);
        EphemeralKey eKey = new EphemeralKey(Provisioning.A_zimbraAuthTokens, "8.7.0_GA_1659");
        EphemeralInput input = new EphemeralInput(eKey, "8.7.0_GA_1659", exp);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        SSDBEphemeralStore store = (SSDBEphemeralStore)SSDBEphemeralStore.getFactory().getStore();
        assertEquals(String.format("8.7.0_GA_1659|%s", exp.getMillis()), store.toValue(input, accountIDLocation));
    }

    @Test
    public void testCsrfTokenToValue() throws ServiceException {
        Expiration exp = new AbsoluteExpiration(1473761137744L);
        EphemeralKey eKey = new EphemeralKey(Provisioning.A_zimbraCsrfTokenData, "3822663c52f27487f172055ddc0918aa");
        EphemeralInput input = new EphemeralInput(eKey, "69643d33363a30666532376439312d656339342d346534352d383436342d3339326262383736313364383b6578703d31333a313437333735383435373138323b7369643d31303a313135303130393434363b", exp);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        SSDBEphemeralStore store = (SSDBEphemeralStore)SSDBEphemeralStore.getFactory().getStore();
        assertEquals(String.format("69643d33363a30666532376439312d656339342d346534352d383436342d3339326262383736313364383b6578703d31333a313437333735383435373138323b7369643d31303a313135303130393434363b|%s", exp.getMillis()), store.toValue(input, accountIDLocation));
    }

    @Test
    public void testCsrfTokenToKey() throws ServiceException {
        Expiration exp = new AbsoluteExpiration(1473761137744L);
        EphemeralKey eKey = new EphemeralKey(Provisioning.A_zimbraCsrfTokenData, "3822663c52f27487f172055ddc0918aa");
        EphemeralInput input = new EphemeralInput(eKey, "69643d33363a30666532376439312d656339342d346534352d383436342d3339326262383736313364383b6578703d31333a313437333735383435373138323b7369643d31303a313135303130393434363b", exp);

        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        SSDBEphemeralStore store = (SSDBEphemeralStore)SSDBEphemeralStore.getFactory().getStore();
        assertEquals("account|47e456be-b00a-465e-a1db-4b53e64fa|zimbraCsrfTokenData|3822663c52f27487f172055ddc0918aa", store.toKey(input, accountIDLocation));
    }

    static class MockAbsoluteExpiration extends AbsoluteExpiration {

        public MockAbsoluteExpiration(Long expiresIn) {
            super(expiresIn);
        }

        @Override
        public long getRelativeMillis() {
            return getMillis();
        }

    }
}
