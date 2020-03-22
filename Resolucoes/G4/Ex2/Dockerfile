FROM bde2020/hadoop-base
COPY target/GGCD_G4-1.0-SNAPSHOT.jar /
ENTRYPOINT ["hadoop", "jar", "/GGCD_G4-1.0-SNAPSHOT.jar", "ggcd.LoadActors"]