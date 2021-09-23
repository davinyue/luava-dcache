package org.rdlinux.luava.dcache.topic;

import java.io.Serializable;
import java.util.Set;

public class OpsValueDeleteMsg implements Serializable {
    private static final long serialVersionUID = -4615901717947105870L;
    private Set<String> keys;

    public OpsValueDeleteMsg(Set<String> keys) {
        this.keys = keys;
    }

    public OpsValueDeleteMsg() {
    }

    public Set<String> getKeys() {
        return this.keys;
    }

    public void setKeys(Set<String> keys) {
        this.keys = keys;
    }
}
