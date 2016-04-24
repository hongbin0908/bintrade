CREATE TABLE fex.index_SP500_list2 (
    ticker string,
    code    string,
    name    string,
    sector  string
)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
