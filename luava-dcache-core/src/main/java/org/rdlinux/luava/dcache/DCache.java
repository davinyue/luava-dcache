package org.rdlinux.luava.dcache;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

public interface DCache {
    <Key, Value> Value get(Key key);

    <Key, Value> Value get(Key key, Function<Key, Value> call);

    <Key, Value> Map<Key, Value> multiGet(Collection<Key> keys);

    <Key, Value> Map<Key, Value> multiGet(Collection<Key> keys, Function<Collection<Key>, Map<Key, Value>> call);

    <Key, Value> void set(Key key, Value value);

    <Key, Value> void multiSet(Map<Key, Value> kvs);

    <Key> void delete(Key key);

    <Key> void multiDelete(Collection<Key> keys);

    <Key> void multiDelete(Key[] keys);
}
