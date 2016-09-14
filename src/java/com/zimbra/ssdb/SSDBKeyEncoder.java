package com.zimbra.ssdb;

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.zimbra.cs.ephemeral.EphemeralKey;
import com.zimbra.cs.ephemeral.EphemeralLocation;
import com.zimbra.cs.ephemeral.KeyEncoder;

public class SSDBKeyEncoder extends KeyEncoder {

    @Override
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
