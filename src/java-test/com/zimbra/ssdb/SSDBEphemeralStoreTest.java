package com.zimbra.ssdb;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import static org.easymock.EasyMock.*;

import org.easymock.*;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.ephemeral.EphemeralInput;
import com.zimbra.cs.ephemeral.EphemeralLocation;
import com.zimbra.cs.ephemeral.EphemeralResult;
import com.zimbra.cs.ephemeral.EphemeralStore;
import com.zimbra.cs.ephemeral.InMemoryEphemeralStore;
import com.zimbra.cs.ephemeral.EphemeralInput.Expiration;
import com.zimbra.cs.mailbox.MailboxTestUtil;

public class SSDBEphemeralStoreTest {

    @Mock
    private JedisPool mockJedisPool;
    
    @Mock
    private Jedis jedis;
    
    @Before
    public void setUp() throws Exception {
        jedis = EasyMock.mock(Jedis.class);
        mockJedisPool = EasyMock.mock(JedisPool.class);
        MailboxTestUtil.initServer("../zm-store/");
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
        
        expect(mockJedisPool.getResource()).andReturn(jedis).atLeastOnce();
        expect(jedis.get("somekey")).andReturn(null);
        jedis.close();
        replay(mockJedisPool);
        replay(jedis);
        store.get("somekey", null);
        verify(mockJedisPool);
        verify(jedis);
    }
    
    @Test
    public void testSet() throws ServiceException {
        EphemeralStore.setFactory(SSDBEphemeralStore.Factory.class);
        EphemeralStore store = SSDBEphemeralStore.getFactory().getStore();
        assertTrue(store instanceof SSDBEphemeralStore);
        
        EphemeralInput kv = new EphemeralInput("testk","testv");
        ((SSDBEphemeralStore)store).setPool(mockJedisPool);
        
        expect(mockJedisPool.getResource()).andReturn(jedis).atLeastOnce();
        expect(jedis.set("testk","testv")).andReturn("testk");
        jedis.close();
        replay(mockJedisPool);
        replay(jedis);
        store.set(kv, null);
        verify(mockJedisPool);
        verify(jedis);
    }
    
    @Test
    public void testAuthTokenToKey() throws ServiceException {
        Expiration exp = new Expiration(1473761137744L, TimeUnit.MILLISECONDS);
        EphemeralInput input = new EphemeralInput("zimbraAuthTokens", "366778080|8.7.0_GA_1659", exp, true);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        assertEquals("47e456be-b00a-465e-a1db-4b53e64fa|366778080|8.7.0_GA_1659", SSDBEphemeralStore.toKey(input, accountIDLocation));
    }
    
    @Test
    public void testAuthTokenToValue() throws ServiceException {
        Expiration exp = new Expiration(1473761137744L, TimeUnit.MILLISECONDS);
        EphemeralInput input = new EphemeralInput("zimbraAuthTokens", "366778080|8.7.0_GA_1659", exp, true);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        assertEquals("", SSDBEphemeralStore.toValue(input, accountIDLocation));
    }
    
    @Test
    public void testCsrfTokenToValue() throws ServiceException {
        Expiration exp = new Expiration(1473761137744L, TimeUnit.MILLISECONDS);
        EphemeralInput input = new EphemeralInput("zimbraAuthTokens", "69643d33363a30666532376439312d656339342d346534352d383436342d3339326262383736313364383b6578703d31333a313437333735383435373138323b7369643d31303a313135303130393434363b:3822663c52f27487f172055ddc0918aa", exp, true);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        assertEquals("69643d33363a30666532376439312d656339342d346534352d383436342d3339326262383736313364383b6578703d31333a313437333735383435373138323b7369643d31303a313135303130393434363b", SSDBEphemeralStore.toValue(input, accountIDLocation));
    }
    
    @Test
    public void testCsrfTokenToKey() throws ServiceException {
        Expiration exp = new Expiration(1473761137744L, TimeUnit.MILLISECONDS);
        EphemeralInput input = new EphemeralInput("zimbraAuthTokens", "69643d33363a30666532376439312d656339342d346534352d383436342d3339326262383736313364383b6578703d31333a313437333735383435373138323b7369643d31303a313135303130393434363b:3822663c52f27487f172055ddc0918aa", exp, true);
        EphemeralLocation accountIDLocation = new EphemeralLocation() {
            @Override
            public String[] getLocation() { return new String[] { "account", "47e456be-b00a-465e-a1db-4b53e64fa" }; }
        };
        assertEquals("47e456be-b00a-465e-a1db-4b53e64fa|3822663c52f27487f172055ddc0918aa", SSDBEphemeralStore.toValue(input, accountIDLocation));
    }
}
