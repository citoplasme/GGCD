FROM bde2020/spark-base:2.4.5-hadoop2.7
COPY target/GGCD_G8-1.0-SNAPSHOT.jar /
ENTRYPOINT ["/spark/bin/spark-submit", "--executor-memory","1g","--driver-memory","1g","--total-executor-cores","2", "--class", "ggcd.Ex1", "--master", "spark://spark-master:7077", "/GGCD_G8-1.0-SNAPSHOT.jar"]
