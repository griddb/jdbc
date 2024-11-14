=============
基本編
=============

基本操作
===========================

基本操作として、接続、SQLの実行、検索結果の取得の3種類あります。

.. csv-table::

    大分類,クラス,メソッド,機能概要
    接続,,connect(),接続
    SQLの実行,Cursor,execute(),SQLの実行
    ,Cursor,executemany(),N個入力のSQLの実行
    検索結果の取得,Cursor,fetchone(),1件取得
    ,Cursor,fetchmany(),取得件数指定の取得
    ,Cursor,fetchall(),全件取得
    
事前処理
===========================

事前に以下のインポートとJVMの起動が必要です。

.. code-block:: python

   import jpype
   import jpype.dbapi2
   jpype.startJVM(classpath=['/usr/share/java/gridstore-jdbc.jar'])

startJVM()メソッドにて、JDBCドライバファイル gridstore-jdbc.jar をクラスパスに指定します。

接続
===========================

connect()メソッドに以下を指定してください。

- url: 接続に使うURL
- driver: "com.toshiba.mwcloud.gs.sql.Driver"
- driver_args: GridDBサーバに接続するためのGridDBユーザ名とパスワード

接続の例

.. code-block:: python

    url = "jdbc:gs://127.0.0.1:20001/myCluster/public"
    conn = jpype.dbapi2.connect(url, driver="com.toshiba.mwcloud.gs.sql.Driver",
	    driver_args={"user":"admin", "password":"admin"})

接続時のURL形式
---------------------------

URLは以下の(A)～(D)の形式となります。クラスタ構成方式がマルチキャスト方式の場合、通常は(A)の形式で接続してください。 GridDBクラスタ側で自動的に負荷分散が行われ適切なノードに接続されます。 GridDBクラスタとの間でマルチキャストでの通信ができない場合のみ、他の形式で接続してください。

(A)マルチキャスト方式のGridDBクラスタの適切なノードへ自動的に接続する場合

.. code-block:: python

    jdbc:gs://(multicastAddress):(portNo)/(clusterName)/(databaseName)
    
- multicastAddress：GridDBクラスタとの接続に使うマルチキャストアドレス。(デフォルト: 239.0.0.1)
- portNo：GridDBクラスタとの接続に使うポート番号。(デフォルト: 41999)
- clusterName：GridDBクラスタのクラスタ名
- databaseName：データベース名。省略した場合はデフォルトデータベース(public)に接続します。

(B)マルチキャスト方式のGridDBクラスタ内のノードに直接接続する場合

.. code-block:: python

    jdbc:gs://(nodeAddress):(portNo)/(clusterName)/(databaseName)
    
- nodeAddress：ノードのアドレス
- portNo：ノードとの接続に使うポート番号。(デフォルト: 20001)
- clusterName：ノードが属するGridDBクラスタのクラスタ名
- databaseName：データベース名。省略した場合はデフォルトデータベース(public)に接続します。

(C)固定リスト方式のGridDBクラスタに接続する場合

クラスタ構成方式が固定リスト方式の場合、この形式で接続してください。

.. code-block:: python

    jdbc:gs:///(clusterName)/(databaseName)?notificationMember=(notificationMember)
    
- clusterName：GridDBクラスタのクラスタ名
- databaseName：データベース名。省略した場合はデフォルトデータベース(public)に接続します。
- notificationMember：ノードのアドレスリスト(URLエンコードが必要)。デフォルトポートは20001
    例：192.168.0.10:20001,192.168.0.11:20001,192.168.0.12:20001

※notificationMemberはgs_cluster.jsonファイルを編集することで変更可能です。 アドレスリストで使うポートは、gs_node.jsonファイルを編集することで変更可能です。

(D)プロバイダ方式のGridDBクラスタに接続する場合

クラスタ構成方式がプロバイダ方式の場合、この形式で接続してください。

.. code-block:: python

    jdbc:gs:///(clusterName)/(databaseName)?notificationProvider=(notificationProvider)
    
- clusterName：GridDBクラスタのクラスタ名
- databaseName：データベース名。省略した場合はデフォルトデータベースに接続します
- notificationProvider：アドレスプロバイダのURL(URLエンコードが必要)

※notificationProviderはgs_cluster.jsonファイルを編集することで変更可能です。

その他情報の設定
---------------------------

接続時には次の情報も設定できます。

.. csv-table::
    :header: 設定項目,プロパティ名,説明
    :widths: 15, 5, 30

    接続タイムアウト,loginTimeout,接続タイムアウト(秒)。デフォルトは300秒
    外部通信経路指定,connectionRoute,外部通信経路を用いた接続を行う場合、PUBLICを指定します                                                 
    マルチキャスト受信I/F指定,notificationNetworkInferfaceAddress,マルチキャストパケットを受信するインターフェースのアドレス
    アプリケーション名,applicationName,アプリケーション名
    タイムゾーン,timeZone,タイムゾーン。サーバ側で指定したタイムゾーンの日時文字列を取得したい場合に指定します

GridDBユーザ名と同様にdriver_args部分に「プロパティ名=設定値」形式で設定できます。
また、接続タイムアウト以外は、URLの末尾に「?プロパティ名=設定値」形式で追記しても設定できます。

SQLの実行
===========================

3つの実行例を示します。

(A) execute()によるSQLの実行例

.. code-block:: python

    curs = conn.cursor()
    curs.execute("CREATE TABLE IF NOT EXISTS Sample ( id integer PRIMARY KEY, value string )")
    curs.execute("INSERT INTO Sample values (0, 'test0')")
    curs.execute("INSERT INTO Sample values (1, 'test1')")
    curs.execute("SELECT * from Sample where id > 0")
    print(curs.fetchall())

サンプルコードはsampleSimple.pyをご参照ください。

(B) execute()によるプレースホルダ付きSQLの実行例

プレースホルダは「?」記号を使います。

.. code-block:: python

    curs = conn.cursor()
    curs.execute("INSERT INTO Sample values (?, ?)", (0, 'test0'))

サンプルコードはsampleParametered.pyをご参照ください。

(C) executemany()によるプレースホルダ付きSQLの実行例

.. code-block:: python

    curs = conn.cursor()
    data = [ (0, 'test0'), (1, 'test1'), (2, 'test2') ]
    curs.executemany("INSERT INTO Sample values (?, ?)", data)

サンプルコードはsampleMany.pyをご参照ください。

検索結果の取得
===========================

3つの実行例を示します。

(A) 1件取得の例

.. code-block:: python

    curs.execute("SELECT * from Sample where id > 0")
    curs.fetchone()

(B) N件指定での取得の例

.. code-block:: python

    curs.execute("SELECT * from Sample where id > 0")
    curs.fetchmany(10)

(C) 全件取得の例

.. code-block:: python

    curs.execute("SELECT * from Sample where id > 0")
    curs.fetchall()

その他
===========================

以下のプロパティを使うことで、JDBCの直接操作が可能です。

- Conectionクラスのconnectionプロパティ：JDBCのConnectionオブジェクトを取得し、メタデータ取得等のJDBCの操作が可能
- CursorクラスのresultSetプロパティ：JDBCのResultSetオブジェクトを取得し、JDBCの操作が可能

また、データ取得時・データ設定時のGridDBとPythonでのデータ型の対応関係については、
付録の :ref:`getter` および :ref:`setter` をご覧ください。

制限事項
===========================

JPype DBAPI2では以下の操作をサポートしておりません。【JPype DBAPI2仕様】

- setinputsizes(sizes)
- setoutputsize(size [, column])

GridDB PythonAPI(SQL I/F)では以下の操作はサポートしておりません。【GridDB仕様】

- プロシージャコール(callproc())
- commit()およびroolback()
- nextset()およびlastrowidプロパティ

