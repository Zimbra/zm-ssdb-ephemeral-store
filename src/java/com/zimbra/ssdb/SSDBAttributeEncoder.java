package com.zimbra.ssdb;

import com.zimbra.common.service.ServiceException;
import com.zimbra.cs.ephemeral.AttributeEncoder;
import com.zimbra.cs.ephemeral.EphemeralKey;
import com.zimbra.cs.ephemeral.EphemeralKeyValuePair;

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
    public EphemeralKeyValuePair decode(String key, String value) throws ServiceException {
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

        return new EphemeralKeyValuePair(eKey, value);
    }
}
