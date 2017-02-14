package com.zimbra.ssdb;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.ephemeral.AttributeEncoder;
import com.zimbra.cs.ephemeral.EphemeralKey;
import com.zimbra.cs.ephemeral.ExpirableEphemeralKeyValuePair;

/**
 *
 * @author Greg Solovyev
 *
 */
public class SSDBAttributeEncoder extends AttributeEncoder {

    public SSDBAttributeEncoder() {
        setKeyEncoder(new SSDBKeyEncoder());
        setValueEncoder(new SSDBValueEncoder());
    }

    @Override
    public ExpirableEphemeralKeyValuePair decode(String key, String value) throws ServiceException {
        String[] toks = key.split("|");
        if(toks.length < 3) {
            //SSDB uses format "entry type|entry ID|attribute name|optional dynamic part" for the key
            throw ServiceException.PARSE_ERROR(String.format("unable to parse ephemeral key %s", key), null);
        }
        EphemeralKey eKey = null;
        String attrName = toks[2];
        if(toks.length > 3) {
            eKey = new EphemeralKey(attrName, toks[3]);
        } else {
            eKey = new EphemeralKey(attrName);
        }
        String decodedValue;
        Long expires = null;
        if (value.endsWith("|")) {
            //no expiration encoded
            decodedValue = value.substring(0, value.length() - 1);
        } else {
            int lastPipeIdx = value.lastIndexOf("|");
            decodedValue = value.substring(0, lastPipeIdx - 1);
            String expiryStr = value.substring(lastPipeIdx + 1);
            try {
                expires = Long.parseLong(expiryStr);
            } catch (NumberFormatException e) {
                //fall back to the whole string being the value
                decodedValue = value;
            }
        }
        return new ExpirableEphemeralKeyValuePair(eKey, decodedValue, expires);
    }
}
