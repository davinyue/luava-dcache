package org.rdlinux.luava.dcache.core.dcache.topic;

import java.io.Serializable;
import java.util.Set;

/**
 * 删除hash key事件信息
 */
public class DeleteHashKeyMsg implements Serializable {
    private static final long serialVersionUID = -6227732658378012396L;
    private String key;
    private Set<String> hashKeys;

    public DeleteHashKeyMsg() {
    }

    public DeleteHashKeyMsg(String key, Set<String> hashKeys) {
        this.key = key;
        this.hashKeys = hashKeys;
    }

    public String getKey() {
        return this.key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Set<String> getHashKeys() {
        return this.hashKeys;
    }

    public void setHashKeys(Set<String> hashKeys) {
        this.hashKeys = hashKeys;
    }
}
