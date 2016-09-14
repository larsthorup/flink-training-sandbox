package org.apache.flink.quickstart;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CollectionUtil {
    public static <T> List<T> copy(Iterator<T> iter) {
        List<T> copy = new ArrayList<T>();
        while (iter.hasNext())
            copy.add(iter.next());
        return copy;
    }
}
