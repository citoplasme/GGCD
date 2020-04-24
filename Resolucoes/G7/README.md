# Guião Número 7 de GGCD

### Spark Streaming

##### Exercício 1

O primeiro exercício consistiu em armazenar em ficheiros os vários _RDD_ gerados, sendo este armazenamento efetuado a cada 60 segundos.

##### Exercício 2

Aqui pretendia-se calcular o Top 3 de filmes com melhor cotação, existindo uma janela de 10 minutos e sendo o cálculo feito a cada minuto. 

##### Exercício 3

Por fim, neste exercício era pretendido o mesmo que no exercício 2, existindo a nuance de em vez dos identificadores dos filmes, deveriam ser apresentados os nomes dos mesmos. Para isso, foi efetuada uma leitura do ficheiro _title.basics.tsv_ com um _RDD, sendo este armazenado em _cache_, permitindo uma operação de _join_ mais rápida.
