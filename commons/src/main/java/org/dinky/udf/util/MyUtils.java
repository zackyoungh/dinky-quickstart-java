package org.dinky.udf.util;

import java.nio.ByteBuffer;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.BufferUtil;

/**
 * @author ZackYoung
 * @since 1.0.0
 */
public class MyUtils {
    public static ByteBuffer serializeToByteBuffer(Object o) {
        return BufferUtil.create(Convert.toPrimitiveByteArray(o));
    }
}
