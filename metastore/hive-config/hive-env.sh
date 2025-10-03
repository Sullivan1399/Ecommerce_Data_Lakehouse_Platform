# minimal hive-env.sh
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HIVE_CONF_DIR=/opt/hive/conf
export HIVE_LOG_DIR=/opt/hive/logs
# If you want to add JVM args, use a named var:
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Dproc_metastore"
