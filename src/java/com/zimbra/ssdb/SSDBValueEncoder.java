package com.zimbra.ssdb;

import com.zimbra.cs.ephemeral.EphemeralInput;
import com.zimbra.cs.ephemeral.EphemeralLocation;
import com.zimbra.cs.ephemeral.ValueEncoder;

public class SSDBValueEncoder extends ValueEncoder {

    @Override
    public String encodeValue(EphemeralInput input, EphemeralLocation target) {
        return input.getValue().toString();
    }

}
