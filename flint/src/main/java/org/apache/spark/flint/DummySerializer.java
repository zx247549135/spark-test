package org.apache.spark.flint;

import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;

/**
 * Created by Administrator on 2015/5/19.
 */
public class DummySerializer extends Serializer {
    @Override
    public SerializerInstance newInstance() {
        return DummySerializerInstance.INSTANCE;
    }
}
