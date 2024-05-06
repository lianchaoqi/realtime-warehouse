package com.jack.gmall.realtime.common.bean;

/**
 * @BelongsProject: realtime-warehouse
 * @BelongsPackage: com.jack.gmall.realtime.common.bean
 * @Author: lianchaoqi
 * @CreateTime: 2024-05-06  21:15
 * @Description: ~~~~
 * @Version: jdk1.8
 */

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserLoginBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 当天日期
    String curDate;
    // 回流用户数
    Long backCt;
    // 独立用户数
    Long uuCt;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}