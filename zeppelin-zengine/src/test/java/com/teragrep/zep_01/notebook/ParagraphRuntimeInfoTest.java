package com.teragrep.zep_01.notebook;

import jakarta.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ParagraphRuntimeInfoTest {

    @Test
    void asJson() {
        String propertyName = "testPropertyName";
        String label = "testLabel";
        String tooltip = "testTooltip";
        String group = "testGroup";
        String intpSettingId = "testIntpSettingId";
        ParagraphRuntimeInfo info = new ParagraphRuntimeInfo(propertyName,label,tooltip,group,intpSettingId);
        JsonObject json = info.asJson();

        Assertions.assertEquals(propertyName,json.getString("propertyName"));
        Assertions.assertEquals(tooltip,json.getString("tooltip"));
        Assertions.assertEquals(group,json.getString("group"));
        Assertions.assertEquals(intpSettingId,json.getString("interpreterSettingId"));
    }
}