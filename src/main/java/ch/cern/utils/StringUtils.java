package ch.cern.utils;

import java.text.DecimalFormat;
import java.util.Objects;
import java.util.Optional;

public class StringUtils {
	
    private static DecimalFormat DECIMAL_FORMAT = new DecimalFormat(".##");
    
    public static Optional<Character> getLastCharacter(String input) {
        Objects.requireNonNull(input);
        
        int lenght = input.length();
        
        if(lenght == 0)
        		return Optional.empty();

        char lastCharacter = input.charAt(input.length() - 1);
        
        return Optional.of(lastCharacter);
    }
    
	public static String removeQuotes(String input) {
		if(input.charAt(0) == '"' && input.charAt(input.length() - 1) == '"')
			return input.substring(1, input.length() - 1);
		
		if(input.charAt(0) == '\'' && input.charAt(input.length() - 1) == '\'')
			return input.substring(1, input.length() - 1);
		
		return input;
	}
	
	public static String removeSpacesOutsideQuotes(String input) {
		StringBuilder sb = new StringBuilder();
		
		boolean betweenDoubleQuotes = false;
		boolean betweenSimpleQuotes = false;
		
		for (char c : input.toCharArray()) {
			if(c == ' ' && !betweenDoubleQuotes && !betweenSimpleQuotes)
				continue;
			
			sb.append(c);
			
			if(c == '"' && !betweenDoubleQuotes && !betweenSimpleQuotes)
				betweenDoubleQuotes = true;
			else if(c == '\'' && !betweenDoubleQuotes && !betweenSimpleQuotes)
				betweenSimpleQuotes = true;
			else if(c == '"' && betweenDoubleQuotes)
				betweenDoubleQuotes = false;
			else if(c == '\'' && betweenSimpleQuotes)
				betweenSimpleQuotes = false;
		}
		
		return sb.toString();
	}

    public static int countLines(String string) {
        String[] lines = string.split("\n");
        
        if(lines[lines.length-1].trim().length() == 0)
            return lines.length-1;
        
        return lines.length;
    }
    
    public static String headLines(String string, int lines) {
        int fromIndex = 0;
        
        for (int i = 0; i < lines && fromIndex >= 0; i++)
            fromIndex = string.indexOf("\n", fromIndex + 1);
        
        if(fromIndex < 0)
            fromIndex = string.length();
        
        return string.substring(0, fromIndex);
    }

    public static String removeTrailingZerosIfNumber(String input) {
        if(input.matches("^[-]?\\d+\\.[0]+$")) 
            return input.replaceAll("0*$", "").replaceAll("\\.$", "");
        else
            return input;
    }

    public static String asDataAmount(float dataInBytes) {
        String unit = "bytes";
        
        if(dataInBytes == 0)
            return "0 " + unit;
        
        if(dataInBytes / 1000 > 1) {
            dataInBytes /= 1000;
            unit = "KB";
        }
        
        if(dataInBytes / 1000 > 1) {
            dataInBytes /= 1000;
            unit = "MB";
        }
        
        if(dataInBytes / 1000 > 1) {
            dataInBytes /= 1000;
            unit = "GB";
        }
        
        if(dataInBytes / 1000 > 1) {
            dataInBytes /= 1000;
            unit = "TB";
        }
        
        return DECIMAL_FORMAT.format(dataInBytes) + " " + unit;
    }

    public static Object asDataAmountInMb(float dataInBytes) {
        if(dataInBytes == 0)
            return "0 MB";
        
        float dataInMb = dataInBytes / 1000 / 1000; 
        
        return DECIMAL_FORMAT.format(dataInMb) + " MB";
    }

}
