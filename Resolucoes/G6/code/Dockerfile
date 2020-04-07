FROM bde2020/spark-base
COPY target/GGCD_G6_2-1.0-SNAPSHOT.jar /
ENTRYPOINT ["/spark/bin/spark-submit", "--executor-memory","4g","--driver-memory","4g", "--class", "ggcd.Collaborators", "--master", "spark://spark-master:7077", "/GGCD_G6_2-1.0-SNAPSHOT.jar"]