# Guião Número 6 de GGCD

### Spark

##### Exercício 1

O primeiro exercício consistiu em calcular o número de filmes por ator, bem como calcular os 10 atores com mais filmes. Desta forma, o primeiro passo foi retirar a entrada referente ao cabeçalho do ficheiro. Em seguida, filtrar os registos que realmente se tratavam de atores. Dito isto, por cada identificador de ator encontrado é gerado um par com um contador de filmes, sendo que este toma o valor de um por cada filme em que o ator participa. É efetuada a soma destes contadores unitários, sendo seguida de um _swap_ entre a chave e o valor para permitir a ordenação pelo número de filmes. Por fim, é efetuado um novo _swap_, de modo a obter os registos na forma __Ator Número_de_filmes__. De modo a obter o _top 10_, é efetuado um _take_ de 10 unidades ao _RDD_ previamente armazenado em _cache_.

##### Exercício 2

Neste exercício pretendia-se calcular o _top_ 3 de filmes por ator, com base no _rating_ dos mesmos. Para isso, foram definidos dois _jobs_, sendo que o primeiro gerava registos na forma __Filme Ator__ e o segundo na forma __Filme Rating__. Efetuando um _join_ dos dois _jobs_ (previamente armazenados em _cache_), é possível obter entradas do tipo __Ator [(Filme, Rating)]__. Tendo em conta que se pretendia o _top_ 3, era necessário ordenar a lista de pares por cada ator, bem como retirar apenas as 3 primeiras entradas desta, caso existissem. De modo a fazê-lo, é aplicada uma ordenação após uma agrupação pela chave, sendo, depois, seguida da aplicação do método _sublist_ com o tamanho pretendido. 

##### Exercício 3

Por fim, neste exercício era pretendido calcular o conjunto de colaboradores de cada ator, ou seja, o conjunto de atores que trabalhou com um determinado ator nos vários filmes em que participa. Assim, o primeiro passo foi remover o cabeçalho do ficheiro e filtrar apenas os registos que continham atores. Em seguida, foram agrupados os atores por filme, ou seja, __Filme [Ator]__. Tendo os registos neste formato, a lista de atores foi covertida num _set_ de modo a remover repetições, em que cada entrada retratava um par, ou seja, uma relação entre atores. A forma como se obteve estes pares pode ser vista como a aplicação do produto cartesiano à lista previamente obtida. Finalmente, cada um destes pares é promovido a _RDD_, permitindo uma agrupação dos valores pelas chaves, ou seja, os vários identificadores de atores.
