package com.zimbra.ssdb;

import static org.junit.Assert.*;

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
import com.zimbra.cs.ephemeral.EphemeralResult;
import com.zimbra.cs.ephemeral.EphemeralStore;
import com.zimbra.cs.ephemeral.InMemoryEphemeralStore;
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
}
