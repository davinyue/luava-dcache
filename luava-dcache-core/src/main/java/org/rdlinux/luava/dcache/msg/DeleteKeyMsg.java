package org.rdlinux.luava.dcache.msg;

import java.io.Serializable;

/**
 * 删除key事件信息
 */
public class DeleteKeyMsg<Key> implements Serializable {
    private static final long serialVersionUID = -4615901717947105870L;
    private String cacheName;
    private Key key;

    public DeleteKeyMsg(String cacheName, Key key) {
        this.cacheName = cacheName;
        this.key = key;
    }

    public DeleteKeyMsg() {
    }

    public Key getKey() {
        return this.key;
    }

    public void setKey(Key key) {
        this.key = key;
    }

    public String getCacheName() {
        return this.cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }
}
