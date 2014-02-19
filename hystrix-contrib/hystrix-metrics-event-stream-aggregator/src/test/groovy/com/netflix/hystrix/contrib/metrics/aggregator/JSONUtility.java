package com.netflix.hystrix.contrib.metrics.aggregator;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

/**
 * Utility for generating JSON from Maps/Lists and converting JSON to a Map
 * <p>
 * https://gist.github.com/1198069
 */
public class JSONUtility {

    private static Logger logger = Logger.getLogger(JSONUtility.class.getName());

    /**
     * Pass in a Map and this method will return a JSON string.
     * <p>
     * The map can contain Objects, int[], Object[] and Collections and they will be converted into string representations.
     * <p>
     * Nested maps can be included as values and the JSON will have nested object notation.
     * <p>
     * Arrays/Collections can have Maps in them as well.
     * <p>
     * See the unit tests for examples.
     * 
     * @param jsonData
     * @return
     */
    public static String jsonFromMap(Map<String, Object> jsonData) {
        try {
            JsonDocument json = new JsonDocument();
            json.startGroup();

            for (String key : jsonData.keySet()) {
                Object data = jsonData.get(key);
                if (data instanceof Map) {
                    /* it's a nested map, so we'll recursively add the JSON of this map to the current JSON */
                    json.addLiteralValue(key, jsonFromMap((Map<String, Object>) data));
                } else if (data instanceof Object[]) {
                    /* it's an object array, so we'll iterate the elements and put them all in here */
                    json.addLiteralValue(key, "[" + stringArrayFromObjectArray((Object[]) data) + "]");
                } else if (data instanceof Collection) {
                    /* it's a collection, so we'll iterate the elements and put them all in here */
                    json.addLiteralValue(key, "[" + stringArrayFromObjectArray(((Collection) data).toArray()) + "]");
                } else if (data instanceof int[]) {
                    /* it's an int array, so we'll get the string representation */
                    String intArray = Arrays.toString((int[]) data);
                    /* remove whitespace */
                    intArray = intArray.replaceAll(" ", "");
                    json.addLiteralValue(key, intArray);
                } else if (data instanceof JSONCapableObject) {
                    json.addLiteralValue(key, ((JSONCapableObject) data).getJSON());
                } else {
                    /* all other objects we assume we are to just put the string value in */
                    if (data == null) {
                        json.addValue(key, null);
                    } else {
                        json.addValue(key, data);
                    }
                }
            }

            json.endGroup();

            logger.log(Level.FINER, "created json from map => " + json.toString());
            return json.toString();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Could not create JSON from Map. ", e);
            return "{}";
        }
    }

    public static Map<String, Object> mapFromJson(String json) {
        try {
            JSONObject o = new JSONObject(json);
            return mapFromJSONObject(o);
        } catch (JSONException e) {
            throw new RuntimeException("Failed to parse JSON.", e);
        }
    }

    private static Map<String, Object> mapFromJSONObject(JSONObject o) throws JSONException {
        Map<String, Object> jsonMap = new LinkedHashMap<String, Object>();
        // all of the keys are strings so we can ignore this warning
        @SuppressWarnings("unchecked")
        Iterator<String> keys = o.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            Object data = o.get(key);
            if (data instanceof JSONObject) {
                /* it's a nested map, so we'll recursively add the JSON of this map to the current JSON */
                jsonMap.put(key, mapFromJSONObject((JSONObject) data));
            } else if (data instanceof JSONArray) {
                /* it's an object array, so we'll iterate the elements and put them all in here */
                jsonMap.put(key, listFromJSONArray((JSONArray) data));
            } else {
                /* all other objects we assume we are to just put the string value in */
                if (data == JSONObject.NULL) {
                    jsonMap.put(key, null);
                } else if (data instanceof Number || data instanceof Boolean) {
                    jsonMap.put(key, data);
                } else {
                    jsonMap.put(key, stringOf(data));
                }
            }
        }
        return jsonMap;
    }

    private static List<Object> listFromJSONArray(JSONArray array) throws JSONException {
        List<Object> collection = new ArrayList<Object>();
        for (int i = 0; i < array.length(); i++) {
            Object d = array.get(i);
            if (d instanceof JSONObject) {
                collection.add(mapFromJSONObject((JSONObject) d));
            } else if (d instanceof JSONArray) {
                collection.add(listFromJSONArray((JSONArray) d));
            } else {
                collection.add(d);
            }
        }
        return collection;
    }

    /*
     * return a string like: "one","two","three"
     */
    private static String stringArrayFromObjectArray(Object[] data) {
        StringBuilder arrayAsString = new StringBuilder();
        for (Object o : data) {
            if (arrayAsString.length() > 0) {
                arrayAsString.append(",");
            }
            if (o instanceof Map) {
                arrayAsString.append(jsonFromMap((Map<String, Object>) o));
            } else if (o instanceof JSONCapableObject) {
                arrayAsString.append(((JSONCapableObject) o).getJSON());
            } else {
                arrayAsString.append(getJsonStringValue(o));
            }
        }
        return arrayAsString.toString();
    }

    private static String stringOf(Object data) {
        if (data == null) {
            return null;
        } else {
            return String.valueOf(data);
        }
    }

    /**
     * Perform any escaping needed (such as on quotation marks).
     * 
     * @param s
     * @return
     */
    private static String escapeString(String s) {
        return (s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\t", "\\t").replace("\b", "\\b").replace("\n", "\\n").replace("\r", "\\r").replace("\f", "\\f"));
    }

    private static String getJsonStringValue(Object value) {
        StringBuilder json = new StringBuilder();
        if (value == null || value instanceof Number || value instanceof Boolean) {
            json.append(value);
        } else {
            json.append("\"").append(escapeString(String.valueOf(value))).append("\"");
        }
        return json.toString();
    }

    private static class JsonDocument {
        StringBuilder json = new StringBuilder();

        private boolean newGroup = false;

        public JsonDocument startGroup() {
            newGroup = true;
            json.append("{");
            return this;
        }

        public JsonDocument endGroup() {
            json.append("}");
            return this;
        }

        public JsonDocument addValue(String key, Object value) {
            if (!newGroup) {
                // if this is not the first value in a group, put a comma
                json.append(",");
            }
            /* once we're here, the group is no longer "new" */
            newGroup = false;
            /* append the key/value */
            json.append("\"").append(escapeString(key)).append("\"");
            json.append(":");
            json.append(getJsonStringValue(value));
            return this;
        }

        /*
         * THe value is already json-ified, so add it as is.
         */
        public JsonDocument addLiteralValue(String key, String value) {
            if (!newGroup) {
                // if this is not the first value in a group, put a comma
                json.append(",");
            }
            /* once we're here, the group is no longer "new" */
            newGroup = false;
            /* append the key/value */
            json.append("\"").append(escapeString(key)).append("\"");
            json.append(":");
            json.append(value);
            return this;
        }

        public String toString() {
            return json.toString();
        }

    }

    public static class UnitTest {

        // I'm using LinkedHashMap in the testing so I get consistent ordering for the expected results
        @Test
        public void testSimpleOne() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            jsonData.put("myKey", "myValue");

            String json = jsonFromMap(jsonData);
            String expected = "{\"myKey\":\"myValue\"}";

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertEquals(jsonData, parsedData);
        }

        @Test
        public void testSimpleTwo() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            jsonData.put("myKey", "myValue");
            jsonData.put("myKey2", "myValue2");

            String json = jsonFromMap(jsonData);
            String expected = "{\"myKey\":\"myValue\",\"myKey2\":\"myValue2\"}";

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertEquals(jsonData, parsedData);
        }

        @Test
        public void testNull() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            jsonData.put("myKey", "myValue");
            jsonData.put("myKey2", null);

            String json = jsonFromMap(jsonData);
            String expected = "{\"myKey\":\"myValue\",\"myKey2\":null}";

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertNull(parsedData.get("myKey2"));
            assertEquals(jsonData, parsedData);
        }

        @Test
        public void testStringEscape() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            jsonData.put("stringWithEscapeWorthyCharacters", "preamble\r\n\t\f\b\\\"");

            String json = jsonFromMap(jsonData);
            String expected = "{\"stringWithEscapeWorthyCharacters\":\"preamble\\r\\n\\t\\f\\b\\\\\\\"\"}";

            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertEquals("preamble\r\n\t\f\b\\\"", parsedData.get("stringWithEscapeWorthyCharacters"));
            assertEquals(jsonData, parsedData);
        }

        @Test
        public void testStringEnclosedInBraces() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            jsonData.put("stringWithBraces", "{ key : value }");

            String json = jsonFromMap(jsonData);
            String expected = "{\"stringWithBraces\":\"{ key : value }\"}";

            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertEquals("{ key : value }", parsedData.get("stringWithBraces"));
            assertEquals(jsonData, parsedData);

        }

        @Test
        public void testStringEnclosedInBrackets() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            jsonData.put("stringWithBraces", "[ key : value ]");

            String json = jsonFromMap(jsonData);
            String expected = "{\"stringWithBraces\":\"[ key : value ]\"}";

            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertEquals("[ key : value ]", parsedData.get("stringWithBraces"));
            assertEquals(jsonData, parsedData);
        }

        @Test
        public void testLineBreakEscape() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            jsonData.put("stringWithLineBreak", "first line \n second line");

            String json = jsonFromMap(jsonData);
            String expected = "{\"stringWithLineBreak\":\"first line \\n second line\"}";

            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertEquals("first line \n second line", parsedData.get("stringWithLineBreak"));
            assertEquals(jsonData, parsedData);
        }

        @Test
        public void testNestedMapOne() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            jsonData.put("myKey", "myValue");

            Map<String, Object> jsonData2 = new LinkedHashMap<String, Object>();
            jsonData2.put("myNestedKey", "myNestedValue");

            jsonData.put("myNestedData", jsonData2);

            String json = jsonFromMap(jsonData);
            String expected = "{\"myKey\":\"myValue\",\"myNestedData\":{\"myNestedKey\":\"myNestedValue\"}}";

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertEquals(jsonData, parsedData);
        }

        @Test
        public void testNestedMapTwo() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            jsonData.put("myKey", "myValue");

            Map<String, Object> jsonData2 = new LinkedHashMap<String, Object>();
            jsonData2.put("myNestedKey", "myNestedValue");
            jsonData2.put("myNestedKey2", "myNestedValue2");

            jsonData.put("myNestedData", jsonData2);

            String json = jsonFromMap(jsonData);
            String expected = "{\"myKey\":\"myValue\",\"myNestedData\":{\"myNestedKey\":\"myNestedValue\",\"myNestedKey2\":\"myNestedValue2\"}}";

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertEquals(jsonData, parsedData);
        }

        @Test
        public void testArrayOne() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            int[] numbers = { 1, 2, 3, 4 };
            jsonData.put("myKey", numbers);

            String json = jsonFromMap(jsonData);
            String expected = "{\"myKey\":[1,2,3,4]}";

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertTrue(deepEquals(jsonData, parsedData));
        }

        @Test
        public void testArrayTwo() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            String[] values = { "one", "two", "three", "four" };
            jsonData.put("myKey", values);

            String json = jsonFromMap(jsonData);
            String expected = "{\"myKey\":[\"one\",\"two\",\"three\",\"four\"]}";

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertTrue(deepEquals(jsonData, parsedData));
        }

        @Test
        public void testCollectionOne() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            ArrayList<String> values = new ArrayList<String>();
            values.add("one");
            values.add("two");
            values.add("three");
            values.add("four");
            jsonData.put("myKey", values);

            String json = jsonFromMap(jsonData);
            String expected = "{\"myKey\":[\"one\",\"two\",\"three\",\"four\"]}";

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertTrue(deepEquals(jsonData, parsedData));
        }

        @Test
        public void testMapAndList() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            jsonData.put("myKey", "myValue");
            int[] numbers = { 1, 2, 3, 4 };
            jsonData.put("myNumbers", numbers);

            Map<String, Object> jsonData2 = new LinkedHashMap<String, Object>();
            jsonData2.put("myNestedKey", "myNestedValue");
            jsonData2.put("myNestedKey2", "myNestedValue2");
            String[] values = { "one", "two", "three", "four" };
            jsonData2.put("myStringNumbers", values);

            jsonData.put("myNestedData", jsonData2);

            String json = jsonFromMap(jsonData);
            String expected = "{\"myKey\":\"myValue\",\"myNumbers\":[1,2,3,4],\"myNestedData\":{\"myNestedKey\":\"myNestedValue\",\"myNestedKey2\":\"myNestedValue2\",\"myStringNumbers\":[\"one\",\"two\",\"three\",\"four\"]}}";

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertTrue(deepEquals(jsonData, parsedData));
        }

        @Test
        public void testArrayOfMaps() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            ArrayList<Map<String, Object>> messages = new ArrayList<Map<String, Object>>();

            Map<String, Object> message1 = new LinkedHashMap<String, Object>();
            message1.put("a", "valueA1");
            message1.put("b", "valueB1");
            messages.add(message1);

            Map<String, Object> message2 = new LinkedHashMap<String, Object>();
            message2.put("a", "valueA2");
            message2.put("b", "valueB2");
            messages.add(message2);

            jsonData.put("messages", messages);

            String json = jsonFromMap(jsonData);
            String expected = "{\"messages\":[{\"a\":\"valueA1\",\"b\":\"valueB1\"},{\"a\":\"valueA2\",\"b\":\"valueB2\"}]}";

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertTrue(deepEquals(jsonData, parsedData));
        }

        @Test
        public void testPrimitivesNotQuoted() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            jsonData.put("myKey", "myValue");
            jsonData.put("myKey2", 5);

            String json = jsonFromMap(jsonData);
            String expected = "{\"myKey\":\"myValue\",\"myKey2\":5}";

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertEquals("5", String.valueOf(parsedData.get("myKey2")));
            assertTrue(deepEquals(jsonData, parsedData));
            assertEquals(jsonData, parsedData);
        }

        @Test
        public void testPrimitiveArrayNotQuoted() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            jsonData.put("myKey", "myValue");
            ArrayList<Integer> numbers = new ArrayList<Integer>();
            numbers.add(5);
            numbers.add(6);
            numbers.add(7);
            jsonData.put("myKey2", numbers);

            String json = jsonFromMap(jsonData);
            String expected = "{\"myKey\":\"myValue\",\"myKey2\":[5,6,7]}";

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertTrue(deepEquals(jsonData, parsedData));
        }

        @Test
        public void testJsonToMap1() {
            String json = "{\"data\":[\"videos\",32,\"summary\"]}";

            Map<String, Object> jsonMap = mapFromJson(json);
            List list = (List) jsonMap.get("data");
            assertEquals("videos", list.get(0));
            assertEquals(32, list.get(1));
            assertEquals("summary", list.get(2));
        }

        @Test
        public void testJsonToMap2() {
            String json = "{\"data\":[[1,2],[3,4]]}";

            Map<String, Object> jsonMap = mapFromJson(json);
            List list = (List) jsonMap.get("data");
            assertTrue(deepEquals(Arrays.asList(1, 2), list.get(0)));
            assertTrue(deepEquals(Arrays.asList(3, 4), list.get(1)));
        }

        @Test
        public void testJsonToMap3() {
            String json = "{\"data\":[[1,2],{\"key\":3}, 4]}";

            Map<String, Object> jsonMap = mapFromJson(json);
            List list = (List) jsonMap.get("data");
            assertTrue(deepEquals(Arrays.asList(1, 2), list.get(0)));
            Object m = list.get(1);
            assertTrue(m instanceof Map);
            assertEquals(3, ((Map) m).get("key"));
            assertEquals(4, list.get(2));
        }

        @Test
        public void testJsonToMap4() {
            String json = "{\"path\": ['lists',['lolomo','TV Shows'],'length']}";

            Map<String, Object> jsonMap = mapFromJson(json);
            List list = (List) jsonMap.get("path");
            assertEquals("lists", list.get(0));
            assertTrue(deepEquals(Arrays.asList("lolomo", "TV Shows"), list.get(1)));
            assertEquals("length", list.get(2));
        }

        @Test
        public void testMapToJsonEscapesQuotes1() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            jsonData.put("myKey", "hi \"bob\"");

            String json = jsonFromMap(jsonData);
            String expected = "{\"myKey\":\"hi \\\"bob\\\"\"}";
            System.out.println("expected: " + expected);

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertEquals("hi \"bob\"", String.valueOf(parsedData.get("myKey")));
            assertTrue(deepEquals(jsonData, parsedData));
            assertEquals(jsonData, parsedData);
        }

        @Test
        public void testMapToJsonEscapesQuotes2() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            ArrayList<Map<String, Object>> messages = new ArrayList<Map<String, Object>>();

            Map<String, Object> message1 = new LinkedHashMap<String, Object>();
            message1.put("a", "value \"A1\"");
            message1.put("b", "value \"B1\"");
            messages.add(message1);

            Map<String, Object> message2 = new LinkedHashMap<String, Object>();
            message2.put("a", "value \"A2\"");
            message2.put("b", "value \"B2\"");
            messages.add(message2);

            jsonData.put("messages", messages);

            String json = jsonFromMap(jsonData);
            String expected = "{\"messages\":[{\"a\":\"value \\\"A1\\\"\",\"b\":\"value \\\"B1\\\"\"},{\"a\":\"value \\\"A2\\\"\",\"b\":\"value \\\"B2\\\"\"}]}";

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedData = mapFromJson(expected);
            assertTrue(deepEquals(jsonData, parsedData));
        }

        @Test
        public void testJsonCapableObjectAsKeyValue() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            jsonData.put("object", new TestJsonCapableObject());

            String json = jsonFromMap(jsonData);
            String expected = "{\"object\":{\"valueWithLineBreak\":\"firstLine\\nsecondLine\",\"valueWithQuotationMarks\":\"value \\\"inside quotation marks\\\"\"}}";

            // assert writing
            assertEquals(expected, json);

            // assert reading
            Map<String, Object> parsedExpectedData = (Map<String, Object>) mapFromJson(expected).get("object");
            Map<String, Object> parsedRealData = (Map<String, Object>) mapFromJson(json).get("object");

            // reading won't become the same as the original because it can't become a JSONCapableObject
            // so we'll instead manually compare the values from the 'expected' and 'real' maps parsed from JSON

            assertNotNull(parsedExpectedData.get("valueWithLineBreak"));
            assertEquals(parsedExpectedData.get("valueWithLineBreak"), parsedRealData.get("valueWithLineBreak"));
            assertNotNull(parsedExpectedData.get("valueWithQuotationMarks"));
            assertEquals(parsedExpectedData.get("valueWithQuotationMarks"), parsedRealData.get("valueWithQuotationMarks"));

            System.out.println("This should have a line break in it:");
            System.out.println(parsedRealData.get("valueWithLineBreak"));
            System.out.println("");
            System.out.println("This should have quotation marks:");
            System.out.println(parsedRealData.get("valueWithQuotationMarks"));
            System.out.println("");
        }

        @Test
        public void testJsonCapableObjectInList() {
            Map<String, Object> jsonData = new LinkedHashMap<String, Object>();
            ArrayList<Object> listObjects = new ArrayList<Object>();
            listObjects.add(new TestJsonCapableObject());
            jsonData.put("list", listObjects);

            String json = jsonFromMap(jsonData);
            String expected = "{\"list\":[{\"valueWithLineBreak\":\"firstLine\\nsecondLine\",\"valueWithQuotationMarks\":\"value \\\"inside quotation marks\\\"\"}]}";

            // assert writing
            assertEquals(expected, json);

            // assert that we have a list
            assertTrue(mapFromJson(json).get("list") instanceof List);
            assertNotNull(((List) mapFromJson(json).get("list")).get(0));

            // assert reading the Map from the lists first entry.
            Map<String, Object> parsedExpectedData = (Map<String, Object>) ((List<Object>) mapFromJson(expected).get("list")).get(0);
            Map<String, Object> parsedRealData = (Map<String, Object>) ((List<Object>) mapFromJson(json).get("list")).get(0);

            // reading won't become the same as the original because it can't become a JSONCapableObject
            // so we'll instead manually compare the values from the 'expected' and 'real' maps parsed from JSON

            assertEquals(parsedExpectedData.get("valueWithLineBreak"), parsedRealData.get("valueWithLineBreak"));
            assertEquals(parsedExpectedData.get("valueWithQuotationMarks"), parsedRealData.get("valueWithQuotationMarks"));

            System.out.println("This should have a line break in it:");
            System.out.println(parsedRealData.get("valueWithLineBreak"));
            System.out.println("");
            System.out.println("This should have quotation marks:");
            System.out.println(parsedRealData.get("valueWithQuotationMarks"));
            System.out.println("");
        }

        private static class TestJsonCapableObject implements JSONCapableObject {

            @Override
            public String getJSON() {
                return jsonFromMap(asMap());
            }

            private Map<String, Object> asMap() {
                LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();
                map.put("valueWithLineBreak", "firstLine\nsecondLine");
                map.put("valueWithQuotationMarks", "value \"inside quotation marks\"");
                return map;
            }

        }

        /**
         * Used to compare 2 maps deeply and depending on the types.
         * <p>
         * This does not try to be fully comprehensive to all possible types, only to those expected by the maps created by this class
         * 
         * @param m1
         * @param m2
         * @return
         */
        private boolean deepEquals(Map m1, Map m2) {
            for (Object key : m1.keySet()) {
                Object v1 = m1.get(key);
                Object v2 = m2.get(key);
                if (v2 == null) {
                    // missing key, we are not equal
                    System.err.println("Not Equal: v2 is null");
                    return false;
                }
                if (!deepEquals(v1, v2)) {
                    return false;
                }
            }

            // we passed all tests so we declare it true
            return true;
        }

        private boolean deepEquals(Object v1, Object v2) {
            /* convert arrays to List if applicable for comparison */
            v1 = asListIfApplicable(v1);
            v2 = asListIfApplicable(v2);

            if (v1 instanceof Map && v2 instanceof Map) {
                if (!deepEquals((Map) v1, (Map) v2)) {
                    // this child map is not equal
                    return false;
                } else {
                    return true;
                }
            } else if (v1 instanceof Collection && v2 instanceof Collection) {
                if (!Arrays.deepEquals(((Collection) v1).toArray(), ((Collection) v2).toArray())) {
                    // collection not equal
                    return false;
                } else {
                    return true;
                }
            }
            boolean isEqual = v1.equals(v2);
            if (!isEqual) {
                System.err.println("Not Equal: v1: " + v1 + " v2: " + v2);
            }
            return isEqual;
        }

        private Object asListIfApplicable(Object o) {
            if (o instanceof int[]) {
                ArrayList<Integer> l = new ArrayList<Integer>();
                for (int i : (int[]) o) {
                    l.add(i);
                }
                return l;
            } else if (o instanceof String[]) {
                ArrayList<String> l = new ArrayList<String>();
                for (String i : (String[]) o) {
                    l.add(i);
                }
                return l;
            } else if (o instanceof boolean[]) {
                ArrayList<Boolean> l = new ArrayList<Boolean>();
                for (Boolean i : (Boolean[]) o) {
                    l.add(i);
                }
                return l;
            } else if (o instanceof Object[]) {
                ArrayList<Object> l = new ArrayList<Object>();
                for (Object i : (Object[]) o) {
                    l.add(i);
                }
                return l;
            }
            return o;
        }

    }

    public static interface JSONCapableObject {

        String getJSON();

    }

}