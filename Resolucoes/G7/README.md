# Guião Número 7 de GGCD

### Spark Streaming

##### Exercício 1

O primeiro exercício consistiu em armazenar em ficheiros os vários _RDD_ gerados, sendo este armazenamento efetuado a cada 60 segundos. Para isto, foi utilizado o método _saveAsTextFile_ dentro de um ciclo que percorre os _RDD_.

##### Exercício 2

Aqui pretendia-se calcular o _Top_ 3 de filmes com melhor cotação, existindo uma janela de 10 minutos e sendo o cálculo feito a cada minuto. Assim, o _input_ foi mapeado como um par, sendo a chave o identificador do filme e o valor o voto. Agrupando os vários valores pelas chaves, é possível efetuar o cálculo da média, tirando proveito das funcionalidades de _streaming_ de _Java 8_. Em seguida, de modo a ordenar os registos, a ordem do par é alternada, ou seja, a chave passa a ser a média e o valor o identificador do filme. Desta forma, apenas é necessário efetuar um _take_ de 3 unidades para obter os 3 filmes com melhor cotação naquele contexto.

##### Exercício 3

Por fim, neste exercício era pretendido o mesmo que no exercício 2, existindo a nuance de em vez dos identificadores dos filmes, deveriam ser apresentados os nomes dos mesmos. Para isso, foi efetuada uma leitura do ficheiro _title.basics.tsv_ com um _RDD, sendo este armazenado em _cache_, permitindo uma operação de _join_ mais rápida. Com esta junção, os pares passam a possuir o formato __(ID, (Cotação, Nome))__, sendo efetuado um mapeamento para se obter apenas a cotação e nome. Em seguida, os registos são ordenados pela sua chave, ou seja, a cotação, sendo apenas necessário efetuar um _take_ de 3 unidades e apresentar o _Top_ 3.
