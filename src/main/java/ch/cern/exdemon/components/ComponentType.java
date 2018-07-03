package ch.cern.exdemon.components;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import ch.cern.exdemon.components.Component.Type;

@Retention(RetentionPolicy.RUNTIME)
public @interface ComponentType {

	Type value();

}
