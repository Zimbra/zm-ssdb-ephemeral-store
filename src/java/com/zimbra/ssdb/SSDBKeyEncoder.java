package com.zimbra.ssdb;

import java.util.Arrays;

import com.google.common.base.Joiner;
import com.zimbra.cs.ephemeral.EphemeralKey;
import com.zimbra.cs.ephemeral.EphemeralLocation;
import com.zimbra.cs.ephemeral.KeyEncoder;

/**
 * 
 * @author Greg Solovyev
 * SSDBKeyEncoder encodes EphemeralKey and EphemeralLocation.
 */
public class SSDBKeyEncoder extends KeyEncoder {

    @Override
    /**
     * @param target consists of object type and object ID. 
     * @param key consists of attribute name and an optional dynamic part
     * In SSDB, the key will be composed as "object type|object ID|attribute name|dynamic part"
     */
    public String encodeKey(EphemeralKey key, EphemeralLocation target) {
        int pathLength = target.getLocation().length + 1;
        if(key.isDynamic()) {
            pathLength++;
        }
        String[] path = Arrays.copyOf(target.getLocation(), pathLength);
        path[target.getLocation().length] = key.getKey();
        if(key.isDynamic()) {
            path[target.getLocation().length+1] = key.getDynamicComponent();
        }
        return Joiner.on("|").join(path);
    }

}
