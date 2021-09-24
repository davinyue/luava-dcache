package org.rdlinux.luava.dcache.core.dcache;

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
    public static final String Redis_Lock_Prefix = Redis_Prefix + "lock:";
    /**
     * redis缓存前缀
     */
    public static final String Redis_Cache_Prefix = Redis_Prefix + "cache:";
    /**
     * 从redis加载数据的锁前缀
     */
    public static final String Load_From_Redis_Lock_Prefix = Redis_Lock_Prefix + "loadRedis:";
    /**
     * 从call加载数据的锁前缀
     */
    public static final String Load_From_Call_Lock_Prefix = Redis_Lock_Prefix + "loadCall:";

    /**
     * redis主题前缀
     */
    public static final String Redis_Topic_Prefix = Redis_Prefix + "topic:";
}
