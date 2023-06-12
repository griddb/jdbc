package com.toshiba.mwcloud.gs.sql.internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <div lang="ja">
 * @since 5.3
 * </div><div lang="en">
 * @since 5.3
 * </div>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface TimePrecision {

	/**
	 * <div lang="ja">
	 * @since 5.3
	 * </div><div lang="en">
	 * @since 5.3
	 * </div>
	 */
	TimeUnit value();

}
