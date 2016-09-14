package com.zimbra.ssdb;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.ephemeral.EphemeralInput;
import com.zimbra.cs.ephemeral.EphemeralKey;
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
        setAttributeEncoder(new SSDBAttributeEncoder());
    }
    @Override
    public EphemeralResult get(EphemeralKey key, EphemeralLocation location) throws ServiceException {
        EphemeralResult retVal = null;
        String encodedKey = encodeKey(key, location);
        try (Jedis jedis = pool.getResource()) {
            String value = jedis.get(encodedKey);
            if(value != null) {
                retVal = new EphemeralResult(key, value);    
            }
        }
        return retVal;
    }

    @Override
    public void set(EphemeralInput attribute, EphemeralLocation location) throws ServiceException {
        String encodedKey = encodeKey(attribute, location);
        String encodedValue = encodeValue(attribute, location);
        try (Jedis jedis = pool.getResource()) {
            jedis.set(encodedKey, encodedValue);
        }
    }

    @Override
    public void update(EphemeralInput attribute, EphemeralLocation location) throws ServiceException {
        set(attribute, location);
    }


    @Override
    public void delete(EphemeralKey key, String value, EphemeralLocation location) throws ServiceException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean has(EphemeralKey key, EphemeralLocation location) throws ServiceException {
        String encodedKey = encodeKey(key, location);
        try (Jedis jedis = pool.getResource()) {
            String value = jedis.get(encodedKey);
            if(value != null) {
                return true;    
            }
        }
        return false;
    }

    @Override
    public void purgeExpired(EphemeralKey key, EphemeralLocation location) throws ServiceException {
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

    public String toKey(EphemeralInput input, EphemeralLocation location) {
        return encodeKey(input, location);
    }
    
    public String toKey(EphemeralKey key, EphemeralLocation location) {
        return encodeKey(key, location);
    }
    
    public String toValue(EphemeralInput input, EphemeralLocation location) {
        return encodeValue(input, location);
    }
}
