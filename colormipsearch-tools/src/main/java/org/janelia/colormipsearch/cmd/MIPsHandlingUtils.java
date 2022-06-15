package org.janelia.colormipsearch.cmd;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class MIPsHandlingUtils {
    static int extractColorChannelFromMIPName(String mipName) {
        Pattern regExPattern = Pattern.compile("[_-]ch?(\\d+)([_-]|(\\.))", Pattern.CASE_INSENSITIVE);
        Matcher chMatcher = regExPattern.matcher(mipName);
        if (chMatcher.find()) {
            String channel = chMatcher.group(1);
            return Integer.parseInt(channel); // channel base is 0
        } else {
            return -1;
        }
    }

    static String extractObjectiveFromMIPName(String mipName) {
        Pattern regExPattern = Pattern.compile("[_-]([0-9]+x)[_-]", Pattern.CASE_INSENSITIVE);
        Matcher objectiveMatcher = regExPattern.matcher(mipName);
        if (objectiveMatcher.find()) {
            return objectiveMatcher.group(1);
        } else {
            return null;
        }
    }

    static String extractGenderFromMIPName(String mipName) {
        // this assumes the gender is right before the objective
        Pattern regExPattern = Pattern.compile("(m|f)[_-]([0-9]+x)[_-]", Pattern.CASE_INSENSITIVE);
        Matcher objectiveMatcher = regExPattern.matcher(mipName);
        if (objectiveMatcher.find()) {
            return objectiveMatcher.group(1);
        } else {
            return null;
        }
    }
}
