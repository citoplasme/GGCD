# Guião Número 8 de GGCD

### Swarm

O _cluster_ é constituído por 3 máquinas, tendo cada uma delas 2 _cores_ e 7.5GB de memória _RAM_.

##### Exercício 1

Opção em estudo: __--total-executor-cores__

* 1 _Core_  

Ponto de entrada do _Dockerfile_:
```Dockerfile
ENTRYPOINT ["/spark/bin/spark-submit", "--total-executor-cores","1", "--class", "ggcd.Ex1", "--master", "spark://spark-master:7077", "/GGCD_G8-1.0-SNAPSHOT.jar"]
```
Tempo de execução: 226348.0ms

* 2 _Cores_  

Ponto de entrada do _Dockerfile_:
```Dockerfile
ENTRYPOINT ["/spark/bin/spark-submit", "--total-executor-cores","2", "--class", "ggcd.Ex1", "--master", "spark://spark-master:7077", "/GGCD_G8-1.0-SNAPSHOT.jar"]
```
Tempo de execução: 125979.0ms

* 4 _Cores_  

Ponto de entrada do _Dockerfile_:
```Dockerfile
ENTRYPOINT ["/spark/bin/spark-submit", "--total-executor-cores","4", "--class", "ggcd.Ex1", "--master", "spark://spark-master:7077", "/GGCD_G8-1.0-SNAPSHOT.jar"]
```
Tempo de execução: 130287.0ms

##### Exercício 2

Opção em estudo: __--executor-memory__  

* 1 GB de Memória  

Ponto de entrada do _Dockerfile_:
```Dockerfile
ENTRYPOINT ["/spark/bin/spark-submit", "--executor-memory","1g","--driver-memory","1g", "--class", "ggcd.Ex1", "--master", "spark://spark-master:7077", "/GGCD_G8-1.0-SNAPSHOT.jar"]
```
Tempo de execução: 125084.0ms

* 2 GB de Memória  

_Driver Memory_ = 1GB  

Ponto de entrada do _Dockerfile_:
```Dockerfile
ENTRYPOINT ["/spark/bin/spark-submit", "--executor-memory","2g","--driver-memory","1g", "--class", "ggcd.Ex1", "--master", "spark://spark-master:7077", "/GGCD_G8-1.0-SNAPSHOT.jar"]
```
Tempo de execução: 138676.0ms

_Driver Memory_ = 2GB  

Ponto de entrada do _Dockerfile_:
```Dockerfile
ENTRYPOINT ["/spark/bin/spark-submit", "--executor-memory","2g","--driver-memory","2g", "--class", "ggcd.Ex1", "--master", "spark://spark-master:7077", "/GGCD_G8-1.0-SNAPSHOT.jar"]
```
Tempo de execução: 142535.0ms

* 4 GB de Memória  

Ponto de entrada do _Dockerfile_:
```Dockerfile
ENTRYPOINT ["/spark/bin/spark-submit", "--executor-memory","4g","--driver-memory","4g", "--class", "ggcd.Ex1", "--master", "spark://spark-master:7077", "/GGCD_G8-1.0-SNAPSHOT.jar"]
```
Tempo de execução: 183494.0ms


##### Exercício 3

Opção em estudo: _-D dfs.blocksize=n bytes_ (Durante o carregamento dos ficheiros para os nodos de dados)

De modo a comparar efetivamente o impacto do valor de _block size_, foi utilizado o ponto de entrada em seguida em todos os testes deste exercício:  

Ponto de entrada do _Dockerfile_:
```Dockerfile
ENTRYPOINT ["/spark/bin/spark-submit", "--executor-memory","1g","--driver-memory","1g","--total-executor-cores","2", "--class", "ggcd.Ex1", "--master", "spark://spark-master:7077", "/GGCD_G8-1.0-SNAPSHOT.jar"]
```

* Mínimo - 1MB (1048576 bytes)
```bash
curl https://datasets.imdbws.com/title.principals.tsv.gz | gunzip | 
hdfs dfs -D dfs.blocksize=1048576 -put - hdfs://namenode:9000/input/title.principals.tsv 

curl https://datasets.imdbws.com/title.ratings.tsv.gz | gunzip | 
hdfs dfs -D dfs.blocksize=1048576 -put - hdfs://namenode:9000/input/title.ratings.tsv 
```
Tempo de execução: 631466.0ms

* Default - 128MB (134217728)

```bash
curl https://datasets.imdbws.com/title.principals.tsv.gz | gunzip | 
hdfs dfs -put - hdfs://namenode:9000/input/title.principals.tsv 

curl https://datasets.imdbws.com/title.ratings.tsv.gz | gunzip | 
hdfs dfs -put - hdfs://namenode:9000/input/title.ratings.tsv 
```
Tempo de execução: 129942.0ms

* 256MB - 268435456
```bash
curl https://datasets.imdbws.com/title.principals.tsv.gz | gunzip | 
hdfs dfs -D dfs.blocksize=268435456 -put - hdfs://namenode:9000/input/title.principals.tsv 

curl https://datasets.imdbws.com/title.ratings.tsv.gz | gunzip | 
hdfs dfs -D dfs.blocksize=268435456 -put - hdfs://namenode:9000/input/title.ratings.tsv 
```
Tempo de execução: 156678.0ms

##### Exercício Extra

Análise do impacto de utilização de várias opções de execução em simultâneo:

* 2 GB Memória + 4 Cores por Máquina  

Ponto de entrada do _Dockerfile_:
```Dockerfile
ENTRYPOINT ["/spark/bin/spark-submit", "--executor-memory","2g","--driver-memory","2g","--total-executor-cores","4", "--class", "ggcd.Ex1", "--master", "spark://spark-master:7077", "/GGCD_G8-1.0-SNAPSHOT.jar"]
```
Tempo de execução: 139245.0ms

* 1 GB Memória + 2 Cores por Máquina  

Ponto de entrada do _Dockerfile_:
```Dockerfile
ENTRYPOINT ["/spark/bin/spark-submit", "--executor-memory","1g","--driver-memory","1g","--total-executor-cores","2", "--class", "ggcd.Ex1", "--master", "spark://spark-master:7077", "/GGCD_G8-1.0-SNAPSHOT.jar"]
```
Tempo de execução: 129942.0ms

2 GB Memória + 2 Cores por Máquina  

Ponto de entrada do _Dockerfile_:
```Dockerfile
ENTRYPOINT ["/spark/bin/spark-submit", "--executor-memory","2g","--driver-memory","2g","--total-executor-cores","2", "--class", "ggcd.Ex1", "--master", "spark://spark-master:7077", "/GGCD_G8-1.0-SNAPSHOT.jar"]
```
Tempo de execução: 137698.0ms

