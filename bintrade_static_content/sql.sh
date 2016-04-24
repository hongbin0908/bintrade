/opt/spark/bin/spark-submit  --master yarn  \
    --num-executors 3 \
    --driver-memory 256m \
    --executor-memory 256m \
    --executor-cores 1 \
    --files /opt/hive/conf/hive-site.xml \
    --conf hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider \
    ./sql.py

