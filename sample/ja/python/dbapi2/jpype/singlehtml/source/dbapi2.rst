JPype DBAPI2のAPIリファレンス
=============================

`Constructors`
--------------

.. _connect:

.. autofunction:: dbapi2.connect

Globals
-------

JPype dbapi2 は、モジュールの動作を定義するいくつかのグローバルを定義します。
これらの値は定数です。

.. _apilevel:

`apilevel`_
    モジュールのapilevelは「2.0」です。


.. _threadsafety:

`threadsafety`_
    threadsafetyのレベルは 2 で、「スレッドはModuleとConnectionを共有してもよい」
    という意味です。ただし、実際のレベルは、JDBC が接続されているドライバの
    実装によって異なります。
    
    GridDBのJDBCドライバでは、Moduleを共有できますが、Connectionを共有することは
    できません。【GridDB仕様】
    Connectionを作成したスレッド以外のスレッドで、そのConnectionを使用しようとすると、
    エラーが発生します。

    上記のコンテキストでの共有とは、2 つのスレッドが、リソース ロックを実装する
    ためにミューテックス セマフォを使用してリソースをラップすることなく、リソースを
    使用できることを意味します。ミューテックスを使用してアクセスを管理しても、
    外部リソースを常にスレッド セーフにできるわけではないことに注意してください。
    リソースは、制御できないグローバル変数やその他の外部ソースに依存している
    可能性があります。


.. _paramstyle:

`paramstyle`_
    JPype dbapi2モジュールのparamstyleは「qmark」です。

    ============ ==============================================================
    paramstyle   意味
    ============ ==============================================================
    ``qmark``    Question mark style, 例) ``...WHERE name=?``
    ============ ==============================================================

.. _Connection:

`Connection Objects`_
=====================

.. autoclass:: dbapi2.Connection
  :members:

.. _Cursor:

`Cursor Objects`_
=================

.. autoclass:: dbapi2.Cursor
  :members:


`SQL Type Constructor`_
=======================

.. autofunction::  dbapi2.Timestamp
.. autofunction::  dbapi2.TimestampFromTicks
.. autofunction::  dbapi2.Binary

