package ch.cern.components;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import ch.cern.components.Component.Type;

@Retention(RetentionPolicy.RUNTIME)
public @interface ComponentType {

	Type value();

}
