package com.teragrep.zep_01.notebook;

import com.google.common.annotations.VisibleForTesting;
import com.teragrep.zep_01.common.Jsonable;
import jakarta.json.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Store runtime infos of each para
 *
 */
public final class ParagraphRuntimeInfo implements Jsonable {
  private final String propertyName;  // Name of the property
  private final String label;         // Label to be used in UI
  private final String tooltip;       // Tooltip text toshow in UI
  private final String group;         // The interpretergroup from which the info was derived

  // runtimeInfos job url or dropdown-menu key in
  // zeppelin-web/src/app/notebook/paragraph/paragraph-control.html
  private final List<Map<String,String>> values;  // values for the key-value pair property
  private final String interpreterSettingId;
  
  public ParagraphRuntimeInfo(String propertyName, String label, 
      String tooltip, String group, String intpSettingId) {
    if (intpSettingId == null) {
      throw new IllegalArgumentException("Interpreter setting Id cannot be null");
    }
    this.propertyName = propertyName;
    this.label = label;
    this.tooltip = tooltip;
    this.group = group;
    this.interpreterSettingId = intpSettingId;
    this.values = new ArrayList<>();
  }

  public void addValue(Map<String, String> mapValue) {
    values.add(mapValue);
  }

  @VisibleForTesting
  public List<Map<String,String>> getValue() {
    return values;
  }
  
  public String getInterpreterSettingId() {
    return interpreterSettingId;
  }

  @Override
  public JsonObject asJson() {
    final JsonObjectBuilder runtimeInfo = Json.createObjectBuilder();
    final JsonArrayBuilder valuesArrayBuilder = Json.createArrayBuilder();
    for (final Map<String,String> valueMap : values) {
      if(valueMap != null){
        final JsonObjectBuilder valueJson = Json.createObjectBuilder();
        for (final Map.Entry<String,String> entry: valueMap.entrySet()) {
          if(entry.getKey() != null && entry.getValue() != null){
            valueJson.add(entry.getKey(),entry.getValue());
          }
        }
        valuesArrayBuilder.add(valueJson.build());
      }
    }
    final JsonArray valuesArray = valuesArrayBuilder.build();
    runtimeInfo.add("values",valuesArray);
    if(propertyName != null){
      runtimeInfo.add("propertyName",propertyName);
    }
    if(label != null){
      runtimeInfo.add("label",label);
    }
    if(tooltip != null){
      runtimeInfo.add("tooltip",tooltip);
    }
    if(group != null){
      runtimeInfo.add("group",group);
    }
    if(interpreterSettingId != null){
      runtimeInfo.add("interpreterSettingId",interpreterSettingId);
    }
    return runtimeInfo.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ParagraphRuntimeInfo that = (ParagraphRuntimeInfo) o;
    return Objects.equals(propertyName, that.propertyName) && Objects.equals(label, that.label) && Objects.equals(tooltip, that.tooltip) && Objects.equals(group, that.group) && Objects.equals(values, that.values) && Objects.equals(interpreterSettingId, that.interpreterSettingId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(propertyName, label, tooltip, group, values, interpreterSettingId);
  }
}
