package ch.cern.spark;

public class StringUtils {
	
    public static char getLastCharacter(String input) {
        if(input == null)
            throw new IllegalArgumentException("Input is null");
        
        int lenght = input.length();
        
        if(lenght == 0)
            throw new IllegalArgumentException("Input has lenght 0");

        char lastCharacter = input.charAt(input.length() - 1);
        
        return lastCharacter;
    }

    public static String secondsToString(long seconds) {
        if(seconds == 0)
            return "0 seconds";
        
        int hours = (int) (seconds / 3600);
        seconds -= 3600 * hours;
        int minutes = (int) (seconds / 60);
        seconds -= 60 * minutes;
        
        String hours_s = null;
        if(hours > 1) 
            hours_s = hours + " hours";
        else if(hours == 1)
            hours_s = "1 hour";
        
        String minutes_s = null;
        if(minutes > 1) 
            minutes_s = minutes + " minutes";
        else if(minutes == 1)
            minutes_s = "1 minute";
        
        String seconds_s = null;
        if(seconds > 1) 
            seconds_s = seconds + " seconds";
        else if(seconds == 1)
            seconds_s = "1 second";
        
        if(hours_s != null && minutes_s == null && seconds_s == null)
            return hours_s;
        if(hours_s == null && minutes_s != null && seconds_s == null)
            return minutes_s;
        if(hours_s == null && minutes_s == null && seconds_s != null)
            return seconds_s;
        
        if(hours_s != null && minutes_s != null && seconds_s == null)
            return hours_s + " and " + minutes_s;
        if(hours_s == null && minutes_s != null && seconds_s != null)
            return minutes_s + " and " + seconds_s;
        if(hours_s != null && minutes_s == null && seconds_s != null)
            return hours_s + " and " + seconds_s;
        
        return hours_s + ", " + minutes_s + " and " + seconds_s;
    }
    
}
