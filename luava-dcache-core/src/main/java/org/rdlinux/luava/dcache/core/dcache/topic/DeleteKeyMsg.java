package org.rdlinux.luava.dcache.core.dcache.topic;

import java.io.Serializable;
import java.util.Set;

/**
 * 删除key事件信息
 */
public class DeleteKeyMsg implements Serializable {
    private static final long serialVersionUID = -4615901717947105870L;
    private Set<String> keys;

    public DeleteKeyMsg(Set<String> keys) {
        this.keys = keys;
    }

    public DeleteKeyMsg() {
    }

    public Set<String> getKeys() {
        return this.keys;
    }

    public void setKeys(Set<String> keys) {
        this.keys = keys;
    }
}
