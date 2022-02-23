package org.rdlinux.luava.dcache;

/**
 * 常量
 */
public class DCacheConstant {
    /**
     * redis全局公共前缀
     */
    public static final String Redis_Prefix = "ldc:";
    /**
     * redis全局锁前缀
     */
    public static final String REDIS_LOCK_PREFIX = Redis_Prefix + "lock:";
}
