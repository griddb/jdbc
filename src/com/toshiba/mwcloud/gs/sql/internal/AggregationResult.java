/*
   Copyright (c) 2017 TOSHIBA Digital Solutions Corporation

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.toshiba.mwcloud.gs.sql.internal;

import java.util.Date;

/**
 * <div lang="ja">
 * 集計演算の結果を保持します。
 *
 * <p>集計演算に関するクエリの実行もしくは
 * {@link TimeSeries#aggregate(Date, Date, String, Aggregation)}
 * により取得できる、集計演算の結果を保持します。
 * 整数型カラムに対する演算結果を浮動小数点型として、また、有効桁数の
 * 少ない数値型のカラムに対する演算結果をより桁数の多い数値型として
 * 受け取ることができます。</p>
 *
 * <p>保持する型は、集計演算の種別や集計対象のカラムの型によって決定されます。
 * 具体的な規則は{@link Aggregation}またはTQLの仕様を参照してください。</p>
 *
 * <p>取り出しできる型は、保持されている型によって決まります。
 * 保持されている型が数値型の場合はDOUBLE型またはLONG型、TIMESTAMP型の
 * 場合はTIMESTAMP型の値としてのみ取り出しできます。</p>
 * </div><div lang="en">
 * Stores the result of an aggregation operation.
 *
 * <p>Stores the result returned by an aggregation Query or
 * {@link TimeSeries#aggregate(Date, Date, String, Aggregation)}.
 * A floating-point-type result can be obtained from an operation
 * on a numeric-type Column, and a higher-precision result can be obtained
 * from an operation on a numeric-type Column with a small number of
 * significant digits.</p>
 *
 * <p>The type of the stored result depends on the type of aggregation operation and
 * the type of the target Columns. For specific rules, see {@link Aggregation} or
 * the TQL specifications.</p>
 *
 * <p>The type of obtaining value depends on the stored type.
 * DOUBLE type and LONG type are only available when a result is of numeric type,
 * and TIMESTAMP type when a result is of TIMESTAMP type.</p>
 * </div>
 */
interface AggregationResult {

}
