
apilevel = "2.0"
threadsafety = 2
paramstyle = 'qmark'

###############################################################################
# Connection

def connect(dsn, *, driver=None, driver_args=None):
    """ データベースへの接続を作成します。

    パラメータ:【JPype DBAPI2仕様】
       dsn (str): JDBC のデータベース接続文字列(URL)。
       
       driver (str, optional): ロードする JDBC ドライバ。"com.toshiba.mwcloud.gs.sql.Driver"を指定してください。【GridDB仕様】
       
       driver_args (dict): ドライバへの引数。

    例外:
       接続を確立できない場合はエラーになります。

    戻り値:
       成功した場合、新しいConnectionオブジェクトを返します。
    """

    return Connection(connection, adapters, converters, setters, getters)


class Connection(object):
    """ 接続により、JDBC データベースへのアクセスが提供されます。

    接続は管理され、Python の with ステートメントの一部として使用できます。
    接続は、ガベージ コレクションされたとき、with ステートメントのスコープ
    の終了時、または手動で閉じられたときに自動的に閉じられます。
    接続が閉じられると、データベースを使用するすべての操作でエラーが発生します。
    """

    def close(self):
        """ (.__del__() が呼び出されるたびにではなく、)すぐに接続を閉じます。

        この時点から接続は使用できなくなります。
        接続で何らかの操作を行おうとすると、Error (またはサブクラス) 例外が発生します。
        接続を使用しようとしているすべてのカーソルオブジェクトにも同じことが適用されます。
        """

    def commit(self):
        """コミット

        未サポートです。実行するとエラーになります。【GridDB仕様】
        """

    def rollback(self):
        """ロールバック

        未サポートです。実行するとエラーになります。【GridDB仕様】

        """

    def cursor(self):
        """ 新しいカーソル オブジェクトを返します。 """
        return Cursor(self)

    @property
    def connection(self):
        """ この Python Connectionオブジェクトをサポートする JDBC Connectionオブジェクトを取得します。【JPype DBAPI2仕様】

        これを使用して、DBAPIドライバの範囲外にある追加のメタデータやその他の機能を取得できます。
        """
        return self._jcx

    @property
    def autocommit(self):
        """ bool: 自動コミット動作を制御するプロパティ。【JPype DBAPI2仕様】

        True/Falseの設定操作は可能ですが変更はできません。常にTrueを返します。【GridDB仕様】
        """
        return self._jcx.getAutoCommit()

    @property
    def typeinfo(self):
        """ list: このドライバでサポートされているタイプのリスト。【JPype DBAPI2仕様】

        これはドライバの機能を確認するのに役立ちます。
        """
        return out


###############################################################################
# Cursor


class Cursor(object):
    """ カーソルはクエリを実行し、結果を取得するために使用されます。

    一部は PreparedStatement、一部は ResultSet で、カーソルは両方の組み合わせです。
    JDBCのresultSetオブジェクトには resultSetプロパティでアクセスできます。

    カーソルは管理され、Python の with ステートメントの一部として使用できます。
    カーソルは、ガベージ コレクションされたとき、with ステートメントのスコープの終了時、
    または手動で閉じられたときに自動的に閉じます。
    カーソルが閉じられると、データベースを使用するすべての操作でエラーが発生します。
    """

    @property
    def resultSet(self):
        """ [読み取り専用属性] JDBCのResultSetオブジェクトを取得します。【JPype DBAPI2仕様】

        オブジェクトは、次の``execute*()``メソッド呼び出し時に閉じられます
        """
        return self._resultSet

    @property
    def parameters(self):
        """ 
        [読み取り専用属性] parametersは6項目のシーケンスのシーケンスです。【JPype DBAPI2仕様】

        これらの各シーケンスには、1 つの結果列を記述する情報が含まれています。
        現GridDBでは全て常に同じ値を返します。【GridDB仕様】

        - タイプ名(type_name):常に"UNKNOWN"【GridDB仕様】
        - JDBCタイプ(jdbc_type):常にOTHER【GridDB仕様】
        - パラメータモード(parameter_mode) (1=入力、2=入力/出力、4=出力): 常に1【GridDB仕様】
        - 精度(precision): 常に0【GridDB仕様】
        - スケール(scale): 常に0【GridDB仕様】
        - isNullable(null_ok): 常に1【GridDB仕様】
        
        これは、execute の後にのみ使用できます。【GridDB仕様】
        """
        return desc

    @property
    def description(self):
        """ 
        [読み取り専用属性] descriptionは7項目のシーケンスのシーケンスです。【JPype DBAPI2仕様】

        これらの各シーケンスには、1 つの結果列を記述する情報が含まれています。

        - 名前(name):列名
        - タイプコード(type_code):列のデータ型
        - 表示サイズ(display_size):常に131072【GridDB仕様】
        - 内部サイズ(internal_size):常に131072【GridDB仕様】
        - 精度(precision):TIMESTAMP型の場合は3(ミリ秒精度)または6(マイクロ秒精度)または9(ナノ秒精度)、他の型の場合は0【GridDB仕様】
        - スケール(scale):常に0【GridDB仕様】
        - isNullable(null_ok):カラムにNULL値を許可する定数ResultSetMetaData.columnNullable(=1)、またはカラムにNULL値を許可しない定数columnNoNulls(=0)【GridDB仕様】

        これは、最後のクエリによって結果セット(ResultSet)が生成された場合にのみ使用できます。
        """
        return desc

    @property
    def rowcount(self):
        """ [読み取り専用属性] UPDATE や INSERT などの DML ステートメントの場合に、
        最後の .execute*() が影響を与えた行数を返します。【JPype DBAPI2仕様】

        カーソルに対して .execute*() が実行されていない場合、
        または最後の操作の行数がインターフェイスによって判別できない場合、
        属性は -1 になります。JDBC は SELECT から返される行数の取得をサポートしていないため、
        SELECT ステートメントの後の行数は -1 になります。
        """
        return self._rowcount

    def close(self):
        """ (__del__ が呼び出されるたびに閉じるのではなく、)すぐにカーソルを閉じます。

        この時点からカーソルは使用できなくなります。
        カーソルを使用して何らかの操作を行おうとすると、Error (またはサブクラス) 例外が発生します。
        """

    def callproc(self, procname, parameters=()):
        """ ストアド プロシージャを呼び出します。

        未サポートです。実行するとエラーになります。【GridDB仕様】
        """

    def execute(self, operation, parameters=None):
        """
        データベース操作 (クエリまたはコマンド) を準備して実行します。

        パラメータはシーケンスとして提供され、操作内の変数にバインドされます。
        変数は qmark 表記、すなわち「?」記号で指定されます。【JPype DBAPI2仕様】
        JDBC はマッピング スタイルのパラメータをサポートしていません。

        ステートメントを実行すると行数が更新されます。ステートメントに結果セットがない場合、
        行数は -1 になります。ステートメントは複数の結果セットを生成する場合があります。
        複数の結果セットを走査するには、nextset()メソッドを使用します。

        パラメータ:
           operation (str): 実行されるステートメント。 
           
           parameters (list, optional): ステートメントのパラメータのリスト。
           パラメータの数はステートメントで要求される数と一致する必要があります。
           一致しない場合はエラーが発生します。

        返り値:
           このカーソル。
        """
        return self

    def executemany(self, operation, seq_of_parameters):
        """
        データベース操作 (クエリまたはコマンド) を準備し、
        シーケンス seq_of_parameters で見つかったすべてのパラメータシーケンス
        またはマッピングに対してそれを実行します。

        .execute() と同じコメントがこのメソッドにも適用されます。

        パラメータ:
           operation (str): 実行されるステートメント。
           
           seq_of_parameters (list, optional): ステートメントのパラメータのリストのリスト。
           パラメータの数は、ステートメントで要求される数と一致する必要があります。
           一致しない場合は、エラーが発生します。

        返り値:
           このカーソル。
        """

    def fetchone(self):
        """
        クエリ結果セットの次の行を取得し、単一のシーケンスを返します。
        データがなくなった場合は None を返します。

        .execute*() への以前の呼び出しで結果セットが生成されなかった場合、
        またはまだ呼び出しが発行されていなかった場合は、
        エラー (またはサブクラス) 例外が発生します。

        """

    def fetchmany(self, size=None):
        """ 複数の結果を取得します。

        クエリ結果の次の行セットを取得し、シーケンスのシーケンス (タプルのリストなど) を返します。
        使用可能な行がなくなった場合は、空のシーケンスが返されます。

        呼び出しごとに取得する行数はsizeパラメータで指定します。
        指定しない場合は、カーソルのarraysize属性によって取得する行数が決定されます。
        メソッドは、sizeパラメータで指定された行数をできるだけ多く取得しようとします。
        指定された行数が利用できないために取得できない場合は、返される行数が少なくなる場合があります。

        前回の.execute*()呼び出しで結果セットが生成されなかった場合、
        またはまだ呼び出しが発行されていない場合は、エラー (またはサブクラス) 例外が発生します 。
        """
        return rows

    def fetchall(self):
        """ クエリ結果のすべての（残りの）行を取得し、
        それらをシーケンスのシーケンス（タプルのリストなど）として返します。
        カーソルの arraysize 属性がこの操作のパフォーマンスに影響を与える
        可能性があることに注意してください。

        前回の.execute*()呼び出しで結果セットが生成されなかった場合、
        またはまだ呼び出しが発行されていない場合は、エラー (またはサブクラス) 例外が発生します 。
        """
        return rows

    def nextset(self):
        """ このカーソル内の次の結果セットを取得します。

        未サポートです。常にNoneを返します。【GridDB仕様】
        """

    @property
    def arraysize(self):
        """
        .fetchmany()メソッドで取得する行数を指定します。

        この読み取り/書き込み属性は、.fetchmany()メソッドで一度に取得する行数を指定します。
        デフォルトでは1に設定され、一度に1行取得することを意味します。
        """
        return self._arraysize

    @property
    def lastrowid(self):
        """ 最後に挿入された行の ID を取得します。

        未サポートです。実行するとエラーになります。【GridDB仕様】
        """

    def setinputsizes(self, sizes):
        """ これは、.execute*() を呼び出す前に使用して、
        操作のパラメータのメモリ領域を事前に定義できます。

        サイズはシーケンスとして指定されます。つまり、入力パラメータごとに1つの項目です。
        項目は、使用される入力に対応する Type Object であるか、
        文字列パラメータの最大長を指定する整数である必要があります。
        項目が None の場合、その列に対して事前定義されたメモリ領域は予約されません
        (これは、大きな入力に対して事前定義された領域を回避するのに役立ちます)。

        このメソッドは、.execute*() メソッドが呼び出される前に使用されます。

        (未実装)【JPype DBAPI2仕様】
        """
        pass

    def setoutputsize(self, size, column=None):
        """大きな列 (LONG、BLOB など) のフェッチ用の列バッファ サイズを設定します。

        列は結果シーケンスのインデックスとして指定されます。
        列を指定しないと、カーソル内のすべての大きな列のデフォルト サイズが設定されます。

        (未実装)【JPype DBAPI2仕様】
        """
        pass

###############################################################################
# Factories

def Timestamp(year, month, day, hour, minute, second, nano=0):
    """ この関数は、タイムスタンプ値を保持するオブジェクトを生成します。
    
    ナノ秒精度の値を指定できます。【JPype DBAPI2仕様】
    """
    return _jpype.JClass('java.sql.Timestamp')(year - 1900, month - 1, day, hour, minute, second, nano)


def TimestampFromTicks(ticks):
    """
    この関数は、指定された ticks 値 (エポックからの秒数) から
    タイムスタンプ値を保持するオブジェクトを生成します。
    """
    return Timestamp(*time.localtime(ticks)[:6])


def Binary(data):
    """
    この関数は、バイナリ (長い) 文字列値を保持できるオブジェクトを生成します。
    """
    return _jtypes.JArray(_jtypes.JByte)(data)


