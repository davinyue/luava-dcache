package org.rdlinux.luava.dcache;

/**
 * 双缓存操作
 */
public interface DOpvCache extends OpvCache {
    /**
     * 删除通知
     */
    <Key> void deleteNotice(Key key);
}
