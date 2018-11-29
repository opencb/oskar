package org.opencb.oskar.spark.commons;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;

public class PythonUtils {

    private final ObjectMapper objectMapper;

    public PythonUtils() {
        objectMapper = new ObjectMapper()
                .configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true)
                .configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false)
                .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
    }

    public String toJsonString(Object obj) throws IOException {
        return objectMapper.writeValueAsString(obj);
    }

    public Object toJavaObject(String str, String className) throws ClassNotFoundException, IOException {
        return objectMapper.readValue(str, Class.forName(className));
    }

    public <T> T toJavaObject(String str, Class<T> clazz) throws IOException {
        return objectMapper.readValue(str, clazz);
    }

}
