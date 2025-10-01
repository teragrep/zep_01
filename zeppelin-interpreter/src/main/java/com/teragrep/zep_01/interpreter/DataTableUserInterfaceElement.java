package com.teragrep.zep_01.interpreter;


import jakarta.json.JsonObject;

import java.util.List;

public interface DataTableUserInterfaceElement {

    public List<String> getDatasetAsJSON();
    public JsonObject SearchAndPaginate(int draw, int start, int length, String searchString) throws InterpreterException;
}
