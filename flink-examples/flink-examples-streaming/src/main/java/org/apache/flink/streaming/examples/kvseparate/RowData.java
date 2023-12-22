package org.apache.flink.streaming.examples.kvseparate;

public class RowData {
    private String key;

    private String value;

    public RowData() {}

    public RowData(String key, String value)  {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public static RowData of(String key, String value) {
        return new RowData(key, value);
    }

    @Override
    public String toString() {
        return "RowData{key=" + key + ",value=" + value + "}";
    }
}
