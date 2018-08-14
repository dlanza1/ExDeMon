package ch.cern.utils;

import java.util.Objects;
import java.util.Optional;

public class StringUtils {
	
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

}
