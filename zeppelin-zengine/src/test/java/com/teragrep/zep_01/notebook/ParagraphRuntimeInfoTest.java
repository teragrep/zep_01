package com.teragrep.zep_01.notebook;

import jakarta.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ParagraphRuntimeInfoTest {

    @Test
    void asJson() {
        final String propertyName = "testPropertyName";
        final String label = "testLabel";
        final String tooltip = "testTooltip";
        final String group = "testGroup";
        final String intpSettingId = "testIntpSettingId";
        final ParagraphRuntimeInfo info = new ParagraphRuntimeInfo(propertyName,label,tooltip,group,intpSettingId);
        final JsonObject json = info.asJson();

        Assertions.assertEquals(propertyName,json.getString("propertyName"));
        Assertions.assertEquals(tooltip,json.getString("tooltip"));
        Assertions.assertEquals(group,json.getString("group"));
        Assertions.assertEquals(intpSettingId,json.getString("interpreterSettingId"));
    }
}