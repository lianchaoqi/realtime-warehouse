package com.jack.gmall.realtime.common.function.impl;

import com.alibaba.fastjson.JSONObject;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.common.function.impl
 * @Author: lianchaoqi
 * @CreateTime: 2024-05-15  23:18
 * @Description: ~~~~
 * @Version: jdk1.8
 */
public interface DimAsyncCachejoin<T> {
    String getId(T T);

    String getDimTabName();

    void dimJoin(T T, JSONObject jsonObject);
}
