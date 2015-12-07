package org.apache.eagle.datastream.utils;

import java.lang.reflect.ParameterizedType;

/**
 * @since 12/7/15
 */
class JavaReflections {
    @SuppressWarnings("unchecked")
    public static Class<?> getGenericTypeClass(final Object obj,int index) {
        return (Class<?>) ((ParameterizedType) obj
                .getClass()
                .getGenericSuperclass()).getActualTypeArguments()[index];
    }
}