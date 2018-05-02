package ch.cern.spark.status.storage.manager;

import java.util.regex.Pattern;

import org.apache.spark.api.java.function.Function;

import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import lombok.ToString;
import scala.Tuple2;

@ToString
public class ToStringPatternStatusKeyFilter implements Function<Tuple2<StatusKey, StatusValue>, Boolean> {
    
    private static final long serialVersionUID = -605577220519629679L;
    
    private Pattern pattern;

    public ToStringPatternStatusKeyFilter(String pattern) {
        if(pattern != null)
            this.pattern = Pattern.compile(pattern);
    }

    @Override
    public Boolean call(Tuple2<StatusKey, StatusValue> tuple) throws Exception {
        if(pattern == null)
            return true;
        
        if(tuple._1 == null)
            return false;
        
        return pattern.matcher(tuple._1.toString()).matches();
    }

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ToStringPatternStatusKeyFilter other = (ToStringPatternStatusKeyFilter) obj;
		if (pattern == null) {
			if (other.pattern != null)
				return false;
		} else if (!pattern.pattern().equals(other.pattern.pattern()))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((pattern == null) ? 0 : pattern.pattern().hashCode());
		return result;
	}
    
    

}
