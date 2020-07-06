FROM bde2020/spark-base:2.4.5-hadoop2.7
COPY target/GGCD_TP2-1.0-SNAPSHOT.jar /
ENTRYPOINT ["/spark/bin/spark-submit", "--executor-memory","4g","--driver-memory","4g", "--class", "ggcd.Ex1", "--master", "spark://spark-master:7077", "/GGCD_TP2-1.0-SNAPSHOT.jar"]
#ENTRYPOINT ["/spark/bin/spark-submit", "--executor-memory","4g","--driver-memory","4g", "--class", "ggcd.Ex2A", "--master", "spark://spark-master:7077", "/GGCD_TP2-1.0-SNAPSHOT.jar"]
#ENTRYPOINT ["/spark/bin/spark-submit", "--total-executor-cores","2","--executor-memory","4g","--driver-memory","4g", "--class", "ggcd.Ex2B", "--master", "spark://spark-master:7077", "/GGCD_TP2-1.0-SNAPSHOT.jar"]
#ENTRYPOINT ["/spark/bin/spark-submit", "--executor-memory","4g","--driver-memory","4g", "--class", "ggcd.Ex2C", "--master", "spark://spark-master:7077", "/GGCD_TP2-1.0-SNAPSHOT.jar"]
