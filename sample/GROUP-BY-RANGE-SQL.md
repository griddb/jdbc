
# [A] Aggregation

## Create Table

    CREATE TABLE trend_data1 (
      ts TIMESTAMP PRIMARY KEY,
      value INTEGER
    );

## Insert

    INSERT INTO trend_data1 VALUES(TIMESTAMP('2023-01-01T00:00:00Z'), 10);
    INSERT INTO trend_data1 VALUES(TIMESTAMP('2023-01-01T00:00:10Z'), 30);
    INSERT INTO trend_data1 VALUES(TIMESTAMP('2023-01-01T00:00:20Z'), 30);
    INSERT INTO trend_data1 VALUES(TIMESTAMP('2023-01-01T00:00:30Z'), 50);
    INSERT INTO trend_data1 VALUES(TIMESTAMP('2023-01-01T00:00:40Z'), 50);
    INSERT INTO trend_data1 VALUES(TIMESTAMP('2023-01-01T00:00:50Z'), 70);

## Select

    SELECT ts,avg(value) FROM trend_data1
    WHERE ts BETWEEN TIMESTAMP('2023-01-01T00:00:00Z') AND TIMESTAMP('2023-01-01T00:00:50Z')
    GROUP BY RANGE (ts) EVERY (20,SECOND);

    (result)
    ts                | Col2
    --------------------+-----
    2023-01-01 09:00:00 |   20
    2023-01-01 09:00:20 |   40
    2023-01-01 09:00:40 |   60

# [B] Interporation

## Create Table

    CREATE TABLE trend_data2 (
      ts TIMESTAMP PRIMARY KEY,
      value INTEGER
    );

## Insert

    INSERT INTO trend_data2 VALUES(TIMESTAMP('2023-01-01T00:00:00Z'), 5);
    INSERT INTO trend_data2 VALUES(TIMESTAMP('2023-01-01T00:00:10Z'), 10);
    INSERT INTO trend_data2 VALUES(TIMESTAMP('2023-01-01T00:00:20Z'), 15);

    INSERT INTO trend_data2 VALUES(TIMESTAMP('2023-01-01T00:00:40Z'), 25);

## Select

    SELECT * FROM trend_data2 
    WHERE ts BETWEEN TIMESTAMP('2023-01-01T00:00:00Z') AND TIMESTAMP('2023-01-01T00:00:40Z') 
    GROUP BY RANGE (ts) EVERY (10,SECOND) FILL (LINEAR);

    (result)
    ts                  | value
    --------------------+------
    2023-01-01 09:00:00 |     5
    2023-01-01 09:00:10 |    10
    2023-01-01 09:00:20 |    15
    2023-01-01 09:00:30 |    20
    2023-01-01 09:00:40 |    25
