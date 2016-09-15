package com.zimbra.ssdb;

import com.zimbra.cs.ephemeral.EphemeralInput;
import com.zimbra.cs.ephemeral.EphemeralLocation;
import com.zimbra.cs.ephemeral.ValueEncoder;
/**
 * 
 * @author Greg Solovyev
 *
 */
public class SSDBValueEncoder extends ValueEncoder {

    @Override
    public String encodeValue(EphemeralInput input, EphemeralLocation target) {
        if(input == null || input.getValue() == null) {
            return null;
        } else {
            return input.getValue().toString();
        }
    }
}
