package ch.cern.spark;

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

}
