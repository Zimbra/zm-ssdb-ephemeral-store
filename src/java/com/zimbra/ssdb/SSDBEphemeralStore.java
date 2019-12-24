package com.zimbra.ssdb;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

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
import com.zimbra.cs.ldap.LdapClient;

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
    protected String url;
    public SSDBEphemeralStore(String url) throws ServiceException {
        this.url = url;
        pool = getPool(url);
        setAttributeEncoder(new SSDBAttributeEncoder());
    }

    @Override
    public EphemeralResult get(EphemeralKey key, EphemeralLocation location) throws ServiceException {
        String encodedKey = encodeKey(key, location);
        return new JedisResourceWithRetry<EphemeralResult> () {
            @Override
            public EphemeralResult jedisMethod() throws JedisException, ServiceException {
                try (Jedis jedis = pool.getResource()) {
                    String encodedValue = jedis.get(encodedKey);
                    if(encodedValue != null) {
                        EphemeralKeyValuePair kvp = decode(encodedKey, encodedValue);
                        return new EphemeralResult(key, kvp.getValue());
                    }
                    return EphemeralResult.emptyResult(key);
                }
            }
        }.callMethod();
    }

    @Override
    public void set(EphemeralInput attribute, EphemeralLocation location) throws ServiceException {
        String encodedKey = encodeKey(attribute, location);
        String encodedValue = encodeValue(attribute, location);
        if(encodedValue != null) {
            if(attribute.getExpiration() == null) {
                new JedisResourceWithRetry<String> () {
                    @Override
                    public String jedisMethod() throws JedisException {
                        try (Jedis jedis = pool.getResource()) {
                            return jedis.set(encodedKey, encodedValue);
                        }
                    }
                }.callMethod();
            } else {
                int ttl = (int)(attribute.getRelativeExpiration()/1000);
                if(ttl > 0) {
                    new JedisResourceWithRetry<String> () {
                        @Override
                        public String jedisMethod() throws JedisException {
                            try (Jedis jedis = pool.getResource()) {
                                return jedis.setex(encodedKey, ttl, encodedValue);
                            }
                        }
                    }.callMethod();
                }
            }
        } else {
            this.delete(attribute.getEphemeralKey(), "", location);
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
        new JedisResourceWithRetry<Long> () {
            @Override
            public Long jedisMethod() throws JedisException {
                try (Jedis jedis = pool.getResource()) {
                    return jedis.del(encodedKey);
                }
            }
        }.callMethod();
    }

    @Override
    public boolean has(EphemeralKey key, EphemeralLocation location) throws ServiceException {
        String encodedKey = encodeKey(key, location);
        return new JedisResourceWithRetry<Boolean> () {
            @Override
            public Boolean jedisMethod() throws JedisException {
                try (Jedis jedis = pool.getResource()) {
                    String value = jedis.get(encodedKey);
                    return (value != null);
                }
            }
        }.callMethod();
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

    public static class Factory extends EphemeralStore.Factory {

        private static SSDBEphemeralStore instance;

        /** Note that this falls back to hard coded defaults if LDAP is unavailable */
        protected static GenericObjectPoolConfig getPoolConfig() throws ServiceException {
            GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
            try {
                LdapClient.initializeIfLDAPAvailable();
            } catch (ServiceException se) {
                ZimbraLog.extensions.info("Problem getting SSDB pool access config params", se);
                // Can happen from installer where LDAP isn't running
                poolConfig.setMaxTotal(-1);
                return poolConfig;
            }
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
            return poolConfig;
        }

        public EphemeralStore getNewStore() throws ServiceException {
            String url;
            try {
                url = getURL();
                if (url != null) {
                    return new SSDBEphemeralStore(url);
                }
            } catch (ServiceException e) {
                ZimbraLog.extensions.error("Could not create a new instance of SSDBEphemeralStore", e);
                throw e;
            }
            ZimbraLog.extensions.debug("No URL found to create instance of SSDBEphemeralStore");
            return null;
        }

        @Override
        public EphemeralStore getStore() {
            synchronized (Factory.class) {
                if (instance == null) {
                    String url;
                    try {
                        url = getURL();
                        if (url != null) {
                            instance = new SSDBEphemeralStore(url);
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
            JedisPool pool = SSDBEphemeralStore.getPool(url);
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

    @Override
    public void deleteData(EphemeralLocation location) throws ServiceException {
        /*
         * The only ephemeral attribute that needs to be explicitly deleted is
         * zimbraLastLogonTimestamp. Auth and CSRF tokens will expire automatically.
         */
        new JedisResourceWithRetry<Long> () {
            @Override
            public Long jedisMethod() throws JedisException {
                try (Jedis jedis = pool.getResource()) {
                    EphemeralKey lastLogonEphemeralKey = new EphemeralKey(Provisioning.A_zimbraLastLogonTimestamp);
                    String encoded = encodeKey(lastLogonEphemeralKey, location);
                    return jedis.del(encoded);
                }
            }
        }.callMethod();
    }

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
            GenericObjectPoolConfig config = Factory.getPoolConfig();
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

    private abstract class JedisResourceWithRetry<T> {
        public abstract T jedisMethod() throws JedisException, ServiceException;
        public final T callMethod() throws ServiceException {
            try {
                return jedisMethod();
            } catch (JedisException e) {
                try {
                    /* Jedis throws an exception when connections in the pool go stale.
                     * Since there is no way to test a connection without trying to send data
                     * this code makes one attempt to retry a failed request. 
                     */
                    pool.destroy();
                    pool = getPool(url);
                    return jedisMethod();
                } catch (JedisException e2) {
                    throw wrapJedisException(e2);
                }
            }
        }
    }
}
