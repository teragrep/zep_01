package com.teragrep.zep_01.notebook;

import jakarta.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;


class ParagraphRuntimeInfosTest {

    @Test
    void asJson() {
        Map<String,ParagraphRuntimeInfo> infoMap = new HashMap<>();
        String propertyName = "testPropertyName";
        String label = "testLabel";
        String tooltip = "testTooltip";
        String group = "testGroup";
        String intpSettingId = "testIntpSettingId";
        ParagraphRuntimeInfo info = new ParagraphRuntimeInfo(propertyName,label,tooltip,group,intpSettingId);
        infoMap.put("info1",info);

        String propertyName2 = "testPropertyName2";
        String label2 = "testLabel2";
        String tooltip2 = "testTooltip2";
        String group2 = "testGroup2";
        String intpSettingId2 = "testIntpSettingId2";
        ParagraphRuntimeInfo info2 = new ParagraphRuntimeInfo(propertyName2,label2,tooltip2,group2,intpSettingId2);
        infoMap.put("info2",info2);

        ParagraphRuntimeInfos runtimeInfos = new ParagraphRuntimeInfos(infoMap);
        JsonObject json = runtimeInfos.asJson();

        Assertions.assertEquals(2,json.size());
        Assertions.assertTrue(json.containsKey("info1"));
        Assertions.assertTrue(json.containsKey("info2"));
    }
}