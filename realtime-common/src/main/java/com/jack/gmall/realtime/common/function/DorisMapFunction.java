package com.jack.gmall.realtime.common.function;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.common.function
 * @Author: lianchaoqi
 * @CreateTime: 2024-05-06  21:25
 * @Description: ~~~~
 * @Version: jdk1.8
 */
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

public class DorisMapFunction<T> implements MapFunction<T, String> {

    @Override
    public String map(T bean) throws Exception {
        SerializeConfig serializeConfig = new SerializeConfig();
        serializeConfig.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
        return JSON.toJSONString(bean, serializeConfig);
    }
}