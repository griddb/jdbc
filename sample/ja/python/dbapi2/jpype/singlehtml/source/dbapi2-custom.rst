=============
応用編
=============

JPype DBAPI2のカスタマイズ機能を使うことで、以下の操作が可能となります。

- TIMESTAMP型のナノ秒精度でのデータ取得
- int/float型でのデータ取得
- BLOB型のデータ設定

JPype DBAPI2をカスタマイズするために、Adapters/Convertersを使うことになります。

- Adapters：パラメータを設定するときに、Python型からJava型に変換するために使用されます。
- Converters：検索結果が生成されるとコンバータを使用して、Java型をPython型に変換し直すことができます。

TIMESTAMP型のナノ秒精度でのデータ取得
=====================================

Connectionオブジェクトの_convertersメンバに以下の設定を追加することで、
ナノ秒精度で定義されたTIMESTAMP型のデータ取得で、ナノ秒を保持することが可能になります。

.. code-block:: python

    import jpype.imports
    import java.sql.Timestamp

    conn._converters[java.sql.Timestamp] = jpype.dbapi2._nop

サンプルコードはsampleTimestamp2-Nano.pyをご参照ください。

Pythonのint/float型でのデータ取得
=================================

Connectionオブジェクトの_convertersメンバに以下の設定を追加することで、
Pythonのint/float型でデータを取得できるようになります。

- GridDBの整数型であるBYTE型・SHORT型・INTEGER型・LONG型についてPythonのint型でのデータ取得
- GridDBのFLOAT型・DOUBLE型についてPythonのfloat型でのデータ取得

.. code-block:: python

    conn._converters[jpype.JByte] = int
    conn._converters[jpype.JShort] = int
    conn._converters[jpype.JInt] = int
    conn._converters[jpype.JLong] = int
    conn._converters[jpype.JFloat] = float
    conn._converters[jpype.JDouble] = float

サンプルコードはsampleTypes-IntFloat.pyをご参照ください。

BLOB型のデータ設定
===========================

Connectionオブジェクトの_adaptersメンバと_default_settersグローバルメンバに対し、
以下の設定を追加することで、GridDBのBLOB型にデータ設定することが可能になります。

(入力がbytes型の場合)

.. code-block:: python

    import jpype.imports
    from javax.sql.rowset.serial import SerialBlob

    conn._adapters[bytes] = SerialBlob
    jpype.dbapi2._default_setters[SerialBlob] = jpype.dbapi2.BLOB


(入力がBinary型の場合)

.. code-block:: python

    import jpype.imports
    from javax.sql.rowset.serial import SerialBlob

    conn._adapters[jpype.JArray(jpype.JByte)] = SerialBlob
    jpype.dbapi2._default_setters[SerialBlob] = jpype.dbapi2.BLOB

入力がbytes型の場合のサンプルコードはsampleBlob.pyをご参照ください。
