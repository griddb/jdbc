================
結果取得の高速化
================

検索結果が大量になる場合、結果取得に時間がかかります。
Apache Arrowを使って結果取得することで、処理時間が短縮される場合があります。
Apache Arrowを使った結果取得の方法には2通りあります。

- 全件一括取得
- 全件分割取得

・JVM起動時のclasspathにJava処理用のjarファイル(gridstore-arrow-jdbc.jar)のパスを追加します。

(Arrow JDBC Adapter)

JPype経由で以下のJavaクラスを利用します。

======================== ==================================================================================
RootAllocator            Arrowを利用するためのアロケータクラス
JdbcToArrow              JDBCドライバのResultSetオブジェクトからArrow用のオブジェクトを作成するためのクラス
JdbcToArrowConfigBuilder JdbcToArrowConfigオブジェクトを生成するためのクラス
======================== ==================================================================================

(PyArrow - Apache Arrow Python bindings)

また、以下のPython上のArrow用オブジェクトを利用します。

=========== ============================================================================
RecordBatch カラム指向のデータ構造。スキーマと各カラムごとのデータリストから構成されます
=========== ============================================================================

全件一括取得
===========================

検索結果を全件一括で取得する操作は以下の通りです。

(A)事前準備

JVM起動後に以下の事前準備を行います。

- pyarrow.jvmとJava処理用クラスをインポートします。
- RootAllocatorオブジェクトを生成します。

.. code-block:: python

    import pyarrow.jvm
    from org.apache.arrow.adapter.jdbc import JdbcToArrow
    from org.apache.arrow.memory import RootAllocator

    ra = RootAllocator(sys.maxsize)

(B)データ取得処理

検索実行後に以下の操作を行います。

- CursorクラスのresultSetプロパティでJDBCドライバのResultSetオブジェクトを取得します。
- ResultSetオブジェクトとRootAllocatorオブジェクトからArrow処理用のイテレータ(sqlToArrowVectorIterator)を取得します。
- イテレータのnext()メソッドで全件一括でデータ取得がなされます。
- pyarrow.jvmのrecord_batch()メソッドでPython上にRecordBatchオブジェクトを生成します。

.. code-block:: python

    result_set = curs.resultSet

    it = JdbcToArrow.sqlToArrowVectorIterator(result_set, ra)
    while it.hasNext():
        root = it.next()
        if root.getRowCount() == 0:
            break
        x = pyarrow.jvm.record_batch(root)
        print(x)

サンプルコードはsampleArrowIteratorAll.pyをご参照ください。

全件分割取得
===========================

検索結果の件数が非常に多い場合、分割して取得することが可能です。

(A)事前準備

JVM起動後に以下の事前準備を行います。

- pyarrow.jvmとJava処理用クラスをインポートします。
- RootAllocatorオブジェクトを生成します。
- JdbcToArrowConfigBuilderオブジェクトを生成し、setTargetBatchSize()メソッドで分割して取得する件数を設定します。
- JdbcToArrowConfigオブジェクトを生成します。

以下、2件づつデータを取得する場合の例です。

.. code-block:: python

    import pyarrow.jvm
    from org.apache.arrow.adapter.jdbc import JdbcToArrowConfigBuilder, JdbcToArrow
    from org.apache.arrow.memory import RootAllocator

    ra = RootAllocator(sys.maxsize)

    config_builder = JdbcToArrowConfigBuilder()
    config_builder.setAllocator(ra)
    config_builder.setTargetBatchSize(2) # 分割して取得する件数を設定
    pyarrow_jdbc_config = config_builder.build()

(B)データ取得処理

検索実行後に以下の操作を行います。

- CursorクラスのresultSetプロパティでJDBCドライバのResultSetオブジェクトを取得します。
- ResultSetオブジェクトとJdbcToArrowConfigオブジェクトからArrow処理用のイテレータ(sqlToArrowVectorIterator)を取得します。
- イテレータのnext()メソッドで上記設定した分割取得件数分のデータ取得がなされます。
- pyarrow.jvmのrecord_batch()メソッドでPython上にRecordBatchオブジェクトを生成します。

.. code-block:: python

    result_set = curs.resultSet

    it = JdbcToArrow.sqlToArrowVectorIterator(result_set, pyarrow_jdbc_config)
    while it.hasNext():
        root = it.next()
        if root.getRowCount() == 0:
            break
        x = pyarrow.jvm.record_batch(root)
        print(x)

サンプルコードはsampleArrowIterator.pyをご参照ください。