package ch.cern.spark;

import java.util.Optional;

public class StringUtils {
	
    public static Optional<Character> getLastCharacter(String input) {
        if(input == null)
            return Optional.empty();
        
        int lenght = input.length();
        
        if(lenght == 0)
        		return Optional.empty();

        char lastCharacter = input.charAt(input.length() - 1);
        
        return Optional.of(lastCharacter);
    }

}
