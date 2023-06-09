## Create Table

    CREATE TABLE tbl1 (
      time TIMESTAMP(9) PRIMARY KEY,
      productName STRING,
      value INTEGER,
      time2 TIMESTAMP(6)
    );

    Note: Please specify precision in the form of TIMESTAMP(n).
    ※ TIMESTAMP(p)の形式で精度を指定してください。
    TIMESTAMP or TIMESTAMP(3): MILLISECOND Precission
    TIMESTAMP(6): MICROSECOND Precission
    TIMESTAMP(9): NANOSECOND Precission

## Insert

    INSERT INTO tbl1 
    VALUES(TIMESTAMP_NS('2023-05-18T10:41:26.123456789Z'), 'display', 1, TIMESTAMP_NS('2023-05-18T10:41:26.123456789Z'));

## Select

    SELECT CAST(time AS STRING),productName,value,CAST(time2 AS STRING) from tbl1;
