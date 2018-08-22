package ch.cern.util.function;

import java.util.function.Predicate;

import lombok.Getter;

public class OrPredicate<T> implements Predicate<T>{
	
    @Getter
	private Predicate<T> pred1, pred2;
	
	public OrPredicate(Predicate<T> pred1, Predicate<T> pred2) {
		this.pred1 = pred1;
		this.pred2 = pred2;
	}

	@Override
	public boolean test(T t) {
		return pred1.test(t) || pred2.test(t);
	}
	
	@Override
	public String toString() {
		return "(" + pred1 + " | " + pred2 + ")";
	}

}
