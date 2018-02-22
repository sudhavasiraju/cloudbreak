package com.sequenceiq.it.cloudbreak;

public class TestUtil {

    private TestUtil() {
    }

    static String createLongString(String character, Integer length) {
        String customLongString = "";
        for (int i = 0; i <= length; i++) {
            customLongString = customLongString.concat(character);
        }
        return customLongString;
    }
}
