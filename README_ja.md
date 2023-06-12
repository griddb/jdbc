GridDB JDBCドライバ

## 概要

GridDBJBCドライバはSQLインタフェースを提供します。  

(追加情報)  
[Maven Central Repository上にGridDB Community Edition用のJarパッケージ](https://search.maven.org/search?q=a:gridstore-jdbc)があります。

## 動作環境

以下の環境でビルドとサンプルプログラムの実行を確認しています。

    OS: CentOS 7.9(x64)
    GridDB server: V5.3 CE(Community Edition)

## クイックスタート

### ビルド

    $ ant
    
を実行すると、binフォルダの下に以下のファイルやリンクが作成されます。

    gridstore-jdbc.jar
    gridstore-jdbc-call-logging.jar

### サンプルプログラムの実行
クラスタ名「myCluster」、マルチキャスト方式を使って、事前にGridDBサーバを起動しておく必要があります。

    $ export CLASSPATH=${CLASSPATH}:./bin/gridstore-jdbc.jar
    $ cp sample/ja/jdbc/JDBCSelect.java .
    $ javac JDBCSelect.java
    $ java JDBCSelect

## ドキュメント
  詳細は以下のドキュメントを参照してください。
  - [JDBCドライバ説明書](https://github.com/griddb/docs-ja/blob/master/manuals/GridDB_JDBC_Driver_UserGuide/toc.md)
  - [SQLリファレンス](https://github.com/griddb/docs-ja/blob/master/manuals/GridDB_SQL_Reference/toc.md)

## コミュニティ
  * Issues  
    質問、不具合報告はissue機能をご利用ください。
  * PullRequest  
    GridDB Contributor License Agreement(CLA_rev1.1.pdf)に同意して頂く必要があります。
    PullRequest機能をご利用の場合はGridDB Contributor License Agreementに同意したものとみなします。

## ライセンス
  CクライアントのライセンスはApache License, version 2.0です。  
  サードパーティのソースとライセンスについては3rd_party/3rd_party.mdを参照ください。
