==================
データ型の対応関係
==================

.. _getter:

JPype DBAPI2 データ取得時
===========================

各GridDB TypeがPython上で取得されるデータ型との対応関係を示します。

============ ======================================
GridDB Type  fetchxxx()メソッドで取得されるデータ型
============ ======================================
STRING       str
------------ --------------------------------------
BOOL         bool
------------ --------------------------------------
BYTE         JShort
SHORT        JShort
INTEGER      JInt
LONG         JLong
------------ --------------------------------------
FLOAT        JDouble
DOUBLE       JDouble
------------ --------------------------------------
TIMESTAMP    datetime.datetime
------------ --------------------------------------
BLOB         bytes
============ ======================================

- JXXX型はJavaのデータ型をラッピングしたデータ型です。
- 日時を扱うデータ型では、タイムゾーンはOS等の設定に従います。
- TIMESTAMP型はマイクロ秒精度のdatetime.datetime型で取得されるため、
  ナノ秒精度のTIMESTAMP型を利用している場合はナノ秒部分がカットされます。
  ナノ秒精度でデータを取得したい場合は応用編をご参照ください。
- GridDB Typeが空間型(GEOMETRY)もしくは配列型の場合はNoneを返します。

.. _setter:

JPype DBAPI2 データ設定時
===========================

Python上で入力するデータ型とデータ設定可能なGridDB Typeとの対応関係を
示します。

.. csv-table::

    "入力データ型","入力データの作成例","STRING","BOOL","BYTE","SHORT","INTEGER","LONG","FLOAT","DOUBLE","TIMESTAMP","BLOB"
    str,"v = str('ABC')","〇","×","×","×","×","×","×","×","×","×"
    JString,"v = jpype.JString('ABC')","〇","×","×","×","×","×","×","×","×","×"
    bool,"v = bool(True)","×","〇","×","×","×","×","×","×","×","×"
    JBoolean,"v = jpype.JBoolean(True)","×","〇","×","×","×","×","×","×","×","×"
    int,"v = int(100)","×","×","〇","〇","〇","〇","〇","〇","×","×"
    JByte,"v = jpype.JByte(100)","×","×","×","×","×","×","×","×","×","×"
    JShort,"v = jpype.JShort(100)","×","×","〇","〇","〇","〇","〇","〇","×","×"
    JInt,"v = jpype.JInt(100)","×","×","〇","〇","〇","〇","〇","〇","×","×"
    JLong,"v = jpype.JLong(100)","×","×","〇","〇","〇","〇","〇","〇","×","×"
    float,"v = float(100.12)","×","×","〇","〇","〇","〇","〇","〇","×","×"
    JFloat,"v = jpype.JFloat(100.12)","×","×","〇","〇","〇","〇","〇","〇","×","×"
    JDouble,"v = jpype.JDouble(100.12)","×","×","〇","〇","〇","〇","〇","〇","×","×"
    datetime,"v = datetime.datetime(2024,9,3,10,55,45)","×","×","×","×","×","×","×","×","〇","×"
    Timestamp,"v = jpype.dbapi2.Timestamp(2024,9,3,10,55,45)","×","×","×","×","×","×","×","×","〇","×"
    TimestampFromTicks,"v = jpype.dbapi2.TimestampFromTicks(3600)","×","×","×","×","×","×","×","×","〇","×"
    bytes,"v = 'abc'.encode()","×","×","×","×","×","×","×","×","×","×"
    Binary,"v = jpype.dbapi2.Binary('abc'.encode())","×","×","×","×","×","×","×","×","×","×"

- 〇はデータ設定できるケースがある場合、×は設定できないことを意味します。
  〇であっても、値を保持できる範囲の違いにより、エラーになる場合や値が丸められる場合があります。
- JByte型はどのGridDB Typeにもデータ設定できません。
- 日時を扱うデータ型では、タイムゾーンはOS等の設定に従います。
- datetime型ではマイクロ秒精度に制限されますが、Timestamp型を使うと、ナノ秒精度のデータを設定できます。
- bytes型/BINARY型はどのGridDB Typeにもデータ設定できませんが、応用編に記載のカスタマイズ機能を使うことで、BLOB型にデータ設定できるようになります。
- GridDB Typeが空間型(GEOMETRY)もしくは配列型へのデータ設定はできません。

Apache Arrow データ取得時
===========================

各GridDB Typeはpyarrowの配列のデータ型で取得されます。

============ ===========================
GridDB Type  pyarrowで取得されるデータ型
============ ===========================
STRING       pyarrow.lib.StringArray     
------------ ---------------------------
BOOL         pyarrow.lib.BooleanArray
------------ ---------------------------
BYTE         pyarrow.lib.Int8Array
SHORT        pyarrow.lib.Int16Array
INTEGER      pyarrow.lib.Int32Array
LONG         pyarrow.lib.Int64Array
------------ ---------------------------
FLOAT        pyarrow.lib.FloatArray
DOUBLE       pyarrow.lib.DoubleArray
------------ ---------------------------
TIMESTAMP    pyarrow.lib.TimestampArray
------------ ---------------------------
BLOB         pyarrow.lib.BinaryArray
============ ===========================

- 取得対象に空間型(GEOMETRY)もしくは配列型を含む場合はエラーとなります。
