package com.zimbra.ssdb;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.google.common.annotations.VisibleForTesting;
import com.zimbra.common.service.ServiceException;
import com.zimbra.common.util.ZimbraLog;
import com.zimbra.cs.account.Config;
import com.zimbra.cs.account.Provisioning;
import com.zimbra.cs.ephemeral.EphemeralInput;
import com.zimbra.cs.ephemeral.EphemeralKey;
import com.zimbra.cs.ephemeral.EphemeralKeyValuePair;
import com.zimbra.cs.ephemeral.EphemeralLocation;
import com.zimbra.cs.ephemeral.EphemeralResult;
import com.zimbra.cs.ephemeral.EphemeralStore;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

/**
 *
 * @author Greg Solovyev
 *
 * SSDBEphemeralStore stores ephemeral attributes in SSDB.
 *
 * Attributes are stored as key-value pairs
 * Example 1:
 * Zimbra auth token with value 366778080 for account with ID 47e456be-b00a-465e-a1db-4b53e64fa will be stored in SSDB as a KV pair
 * with a key that looks like the following
 * "account|47e456be-b00a-465e-a1db-4b53e64fa|zimbraAuthTokens|366778080"
 * and value representing server version part of the token: "8.8.0_GA_1234"
 *
 * Example 3:
 * Zimbra CSRF token with value
 * 69643d33363a30666532376439312d656339342d346534352d383436342d3339326262383736313364383b6578703d31333a313437333735383435373138323b7369643d31303a3131353031303934343a6b
 * and crumb 3822663c52f27487f172055ddc0918aa
 * for account with ID 47e456be-b00a-465e-a1db-4b53e64fa will be stored in SSDB as a KV pair
 * with a key that looks like the following
 * "account|47e456be-b00a-465e-a1db-4b53e64fa|zimbraCsrfTokenData|3822663c52f27487f172055ddc0918aa"
 * and value that looks like the following: "69643d33363a30666532376439312d656339342d346534352d383436342d3339326262383736313364383b6578703d31333a313437333735383435373138323b7369643d31303a3131353031303934343a6b"
 *
 * SSDBEphemeralStore uses SSDB's built-in key expiration for attributes that have a non-zero time to live
 */
public class SSDBEphemeralStore extends EphemeralStore {
    public static String SSDB_EPHEMERAL_STORE = "ssdb";
    private JedisPool pool;
    public SSDBEphemeralStore(JedisPool pool) {
        this.pool = pool;
        setAttributeEncoder(new SSDBAttributeEncoder());
    }
    @Override
    public EphemeralResult get(EphemeralKey key, EphemeralLocation location) throws ServiceException {
        String encodedKey = encodeKey(key, location);
        try (Jedis jedis = pool.getResource()) {
            String encodedValue = jedis.get(encodedKey);
            if(encodedValue != null) {
                EphemeralKeyValuePair kvp = decode(encodedKey, encodedValue);
                return new EphemeralResult(key, kvp.getValue());
            }
        } catch (JedisException e) {
            throw wrapJedisException(e);
        }
        return EphemeralResult.emptyResult(key);
    }

    @Override
    public void set(EphemeralInput attribute, EphemeralLocation location) throws ServiceException {
        String encodedKey = encodeKey(attribute, location);
        String encodedValue = encodeValue(attribute, location);
        try (Jedis jedis = pool.getResource()) {
            if(encodedValue != null) {
                if(attribute.getExpiration() == null) {
                    jedis.set(encodedKey, encodedValue);
                } else {
                    int ttl = (int)(attribute.getRelativeExpiration()/1000);
                    if(ttl > 0) {
                        jedis.setex(encodedKey, ttl, encodedValue);
                    }
                }
            } else {
                this.delete(attribute.getEphemeralKey(), "", location);
            }
        } catch (JedisException e) {
            throw wrapJedisException(e);
        }
    }

    @Override
    public void update(EphemeralInput attribute, EphemeralLocation location) throws ServiceException {
        set(attribute, location);
    }


    @Override
    public void delete(EphemeralKey key, String value, EphemeralLocation location) throws ServiceException {
        EphemeralInput attribute = new EphemeralInput(key, value);
        String encodedKey = encodeKey(attribute, location);
        try (Jedis jedis = pool.getResource()) {
            jedis.del(encodedKey);
        } catch (JedisException e) {
            throw wrapJedisException(e);
        }
    }

    @Override
    public boolean has(EphemeralKey key, EphemeralLocation location) throws ServiceException {
        String encodedKey = encodeKey(key, location);
        try (Jedis jedis = pool.getResource()) {
            String value = jedis.get(encodedKey);
            if(value != null) {
                return true;
            }
        } catch (JedisException e) {
            throw wrapJedisException(e);
        }
        return false;
    }

    @Override
    public void purgeExpired(EphemeralKey key, EphemeralLocation location) throws ServiceException {
        //nothing to do here. SSDB deletes expired keys automagically
    }

    public void setPool(JedisPool pool) {
        this.pool = pool;
    }

    protected JedisPool getPool() {
        return pool;
    }

    private ServiceException wrapJedisException(JedisException e) {
        return ServiceException.FAILURE("unable to perform SSDB operation", e);
    }

    public static class Factory implements EphemeralStore.Factory {

        private static SSDBEphemeralStore instance;

        private static JedisPool getPool(String url) throws ServiceException {
            String host;
            Integer port;
            String[] tokens = url.split(":");
            if (tokens != null && tokens.length >= 2 && tokens[0].equalsIgnoreCase(SSDB_EPHEMERAL_STORE)) {
                host = tokens[1];
                port = null;
                if(tokens.length == 3) {
                    try {
                        port = Integer.parseInt(tokens[2]);
                    } catch (NumberFormatException e) {
                        throw ServiceException.FAILURE(
                                String.format("Failed to parse SSDB port number %s", tokens[2]), e);
                    }
                }
                GenericObjectPoolConfig config = getPoolConfig();
                if(port != null) {
                    return new JedisPool(config, host, port);
                } else {
                    return new JedisPool(config, host);
                }
            } else {
                throw ServiceException.FAILURE(String.format(
                        "SSDB backend URL must be of the form 'ssdb:<host>[:<port>]', got '%s'", url), null);
            }
        }

        private static GenericObjectPoolConfig getPoolConfig() {
            GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
            try {
                Config zimbraConf = Provisioning.getInstance().getConfig();
                int poolSize = zimbraConf.getSSDBResourcePoolSize();
                if (poolSize == 0) {
                    poolConfig.setMaxTotal(-1);
                } else {
                    poolConfig.setMaxTotal(poolSize);
                }
                long timeout = zimbraConf.getSSDBResourcePoolTimeout();
                if (timeout > 0) {
                    poolConfig.setMaxWaitMillis(timeout);
                }
            } catch (Exception e) {
                ZimbraLog.extensions.info("Problem getting SSDB pool access config params", e);
                // Can happen from installer where LDAP isn't running
                poolConfig.setMaxTotal(-1);
            }
            return poolConfig;
        }

        @Override
        public EphemeralStore getStore() {
            synchronized (Factory.class) {
                if (instance == null) {
                    String url;
                    try {
                        url = Provisioning.getInstance().getConfig().getEphemeralBackendURL();
                        if (url != null) {
                            @SuppressWarnings("resource")
                            JedisPool pool = getPool(url);
                            instance = new SSDBEphemeralStore(pool);
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
            //nothing to do here
        }

        @Override
        public synchronized void  shutdown() {
            if(instance != null) {
                instance.getPool().close();
                instance.getPool().destroy();
                instance = null;
            }
        }

        @Override
        public void test(String url) throws ServiceException {
            JedisPool pool = getPool(url);
            try {
                pool.getResource();
            } catch (JedisConnectionException e) {
                throw ServiceException.FAILURE(String.format("could not connect to SSDB on URL '%s'", url) , e);
            } finally {
                pool.close();
            }
        }
    }

    @VisibleForTesting
    public String toKey(EphemeralInput input, EphemeralLocation location) {
        return encodeKey(input, location);
    }

    @VisibleForTesting
    public String toKey(EphemeralKey key, EphemeralLocation location) {
        return encodeKey(key, location);
    }

    @VisibleForTesting
    public String toValue(EphemeralInput input, EphemeralLocation location) {
        return encodeValue(input, location);
    }
}
