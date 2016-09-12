package com.zimbra.ssdb;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.ephemeral.EphemeralInput;
import com.zimbra.cs.ephemeral.EphemeralLocation;
import com.zimbra.cs.ephemeral.EphemeralResult;
import com.zimbra.cs.ephemeral.EphemeralStore;
import com.zimbra.cs.ephemeral.InMemoryEphemeralStore;
import com.zimbra.cs.ephemeral.InMemoryEphemeralStore.Factory;

public class SSDBEphemeralStore extends EphemeralStore {
    public static String SSDB_EPHEMERAL_STORE = "ssdb"; 
    private JedisPool pool;
    public SSDBEphemeralStore(JedisPool pool) {
        this.pool = pool;
    }
    @Override
    public EphemeralResult get(String key, EphemeralLocation location) throws ServiceException {
        EphemeralResult retVal = null;
        try (Jedis jedis = pool.getResource()) {
            String value = jedis.get(key);
            if(value != null) {
                retVal = new EphemeralResult(key, value);    
            }
        }
        return retVal;
    }

    @Override
    public void set(EphemeralInput attribute, EphemeralLocation location) throws ServiceException {
        try (Jedis jedis = pool.getResource()) {
            jedis.set(attribute.getKey(), attribute.getValue().toString());
        }

    }

    @Override
    public void update(EphemeralInput attribute, EphemeralLocation location) throws ServiceException {
        // TODO Auto-generated method stub

    }

    @Override
    public void delete(String key, EphemeralLocation location) throws ServiceException {
        // TODO Auto-generated method stub

    }

    @Override
    public void deleteValue(String key, String value, EphemeralLocation location) throws ServiceException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean hasKey(String key, EphemeralLocation location) throws ServiceException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void purgeExpired(String key, EphemeralLocation location) throws ServiceException {
        // TODO Auto-generated method stub

    }
    
    public void setPool(JedisPool pool) {
        this.pool = pool;
    }
    
    protected JedisPool getPool() {
        return pool;
    }

    public static class Factory implements EphemeralStore.Factory {

        private static SSDBEphemeralStore instance;

        @Override
        public EphemeralStore getStore() {
            synchronized (Factory.class) {
                if (instance == null) {
                    String url;
                    try {
                        url = Provisioning.getInstance().getConfig().getEphemeralBackendURL();
                        if (url != null) {
                            String[] tokens = url.split(":");
                            if (tokens != null && tokens.length > 0) {
                                if(tokens[0].equalsIgnoreCase(SSDB_EPHEMERAL_STORE) && tokens.length >= 2) {
                                    String host = tokens[1];
                                    Integer port = null;
                                    if(tokens.length == 3) {
                                        try {
                                            port = Integer.parseInt(tokens[2]);
                                        } catch (NumberFormatException e) {
                                            ZimbraLog.extensions.error("Failed to parse SSDB port number", e);
                                        }
                                    }
                                    JedisPool pool;
                                    if(port != null) {
                                        pool = new JedisPool(host, port);
                                    } else {
                                        pool = new JedisPool(host);
                                    }
                                    instance = new SSDBEphemeralStore(pool);
                                }
                            }
                        }
                    } catch (ServiceException e) {
                        ZimbraLog.extensions.error("Could not create an instance of SSDBEphemeralStore", e);
                    }
                }
                return instance;
            }
        }

        @Override
        public void startup() {
            
        }

        @Override
        public synchronized void  shutdown() {
            if(instance != null) {
                instance.getPool().close();
                instance.getPool().destroy();
                instance = null;
            }
        }
    }
}
