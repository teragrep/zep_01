package com.teragrep.zep_01.notebook;

import jakarta.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;


class ParagraphRuntimeInfosTest {

    @Test
    void asJson() {
        final Map<String,ParagraphRuntimeInfo> infoMap = new HashMap<>();
        final String propertyName = "testPropertyName";
        final String label = "testLabel";
        final String tooltip = "testTooltip";
        final String group = "testGroup";
        final String intpSettingId = "testIntpSettingId";
        final ParagraphRuntimeInfo info = new ParagraphRuntimeInfo(propertyName,label,tooltip,group,intpSettingId);
        infoMap.put("info1",info);

        final String propertyName2 = "testPropertyName2";
        final String label2 = "testLabel2";
        final String tooltip2 = "testTooltip2";
        final String group2 = "testGroup2";
        final String intpSettingId2 = "testIntpSettingId2";
        final ParagraphRuntimeInfo info2 = new ParagraphRuntimeInfo(propertyName2,label2,tooltip2,group2,intpSettingId2);
        infoMap.put("info2",info2);

        final ParagraphRuntimeInfos runtimeInfos = new ParagraphRuntimeInfos(infoMap);
        final JsonObject json = runtimeInfos.asJson();

        Assertions.assertEquals(2,json.size());
        Assertions.assertTrue(json.containsKey("info1"));
        Assertions.assertTrue(json.containsKey("info2"));
    }
}