=============
はじめに
=============

本ドキュメントでは、GridDB PythonAPIのSQLインタフェースについて説明します。

GridDB PythonAPIのSQLインタフェースは、JPype DBAPI2(OSS)を利用してJDBCドライバにアクセスし、
DBAPI2仕様に準拠した操作を提供します。

また、検索結果が大量ヒット時のデータ取得を高速化するために、Apache Arrow(OSS)の
利用方法についても説明します。

※ DBAPI2: PEP 249 – Python Database API Specification v2.0
    https://peps.python.org/pep-0249/

- Python言語標準のDBアクセス用のAPI仕様

※ JPype:
    https://jpype.readthedocs.io/en/latest/index.html

- Python からJNI(Java Native Interface)経由で Java アクセスを提供するソフト(OSS)

※ JPype DBAPI2: 
    https://jpype.readthedocs.io/en/latest/dbapi2.html

- JPype内のモジュールの1つ。JDBCドライバ上にDBAPI仕様による操作（実装）を提供するソフト(OSS)

※ Apahe Arrow:
    https://arrow.apache.org/

- Apacheプロジェクトの1つ。効率的なデータ交換のために設計された、カラム指向のデータフォーマット、ソフト(OSS)

動作環境
===========================

- OS: Ubunt22.04
- Java: 8
- Python: 3.10

※ 本ガイドで記載している使い方について動作確認をしております。

クライアント側の動作に必要なソフト
==================================

- GridDB JDBCドライバ
- JPype 1.5.0
- (データ取得を高速化したい場合) Apache Arrow 16.0.0
