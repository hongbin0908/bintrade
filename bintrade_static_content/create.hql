use fex;
CREATE TABLE IF NOT EXISTS crawler_yahoo (
    symbol string,
    date string,
    result_str string

ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
