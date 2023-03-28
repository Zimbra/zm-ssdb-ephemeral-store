package com.zimbra.ssdb;

import com.zimbra.common.localconfig.LC;
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
        }

        Long expires = input.getExpiration();
        String value = input.getValue().toString();
        String encoded;

        if (expires != null && expires > 0L) {
            encoded = String.format("%s|%s", value, String.valueOf(expires));
        } else {
            encoded = String.format("%s|", value);
        }

        if (!LC.ssdb_zimbrax_compat.booleanValue()) {
            return encoded;
        }

        if (encoded.startsWith("{") || encoded.startsWith("[")) {
            // Already a JSON object
            return encoded;
        }

        // Wrap in double-quotes b/c ZOK expects a JSON string.
        return String.format("\"%s\"", encoded);
    }
}
