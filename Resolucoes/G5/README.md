# Guião Número 5 de GGCD

### Spark

##### Exercício 1

O exercício 1 consistiu em executar uma sequência de passos em que se pretendia contar o número de filmes de um determinado género. Para isso, cada linha foi dividida por _tabs_, sendo seguido da remoção do cabeçalho do ficheiro. Depois disso, apenas se tratou do campo referente ao género. Sendo que este podia tratar-se de uma lista, foi necessário aplicar um _flatMap_ em que se divide a lista em _N_ entidades. Por fim, a cada um destes valores é associado um contador (valor de 1) e efetuado um _reduce_ pela chave em que se somam os valores de contagem.

##### Exercício 2

Ao executar o código previamente realizado para vários ficheiros, foi notória a rapidez com a qual os dados são processados. Assim, para o ficheiro contendo 0.1% dos dados totais o tempo foi de __1583ms__. Para o ficheiro com 1% dos dados, o tempo de execução foi de __2001ms__. No que toca aos ficheiros completos, no formato _gzip_, o tempo de execução foi de __9532ms__. Já no formato _bz2_, demorou __29318ms__.

#### Exercício 3

Neste exercício a ideia passava por fundir dois ficheiros, um contendo os identificadores dos filmes e os seus _ratings_ e outro contendo os seus nomes. Tirando proveito dos métodos fornecidos pelo _Spark_ para este tipo de situações, apenas foi necessário efetuar um _join_ de ambos os _jobs_. No final, tendo em conta o formato do resultado, foi necessário mapear a lista de resultados, ficando-se, apenas, com o conteúdo da direita do par principal.

#### Exercício 4

Por fim, neste exercício era pretendido ordenar os filmes com _rating_ superior a 9. Para isso, tal como nos casos anteriores, o primeiro passo foi remover o cabeçalho. Em seguida, filtrar as entradas cujo _rating_ fosse inferior a __9.0__, bastando armazenar as restantes numa lista. Tendo em conta que se pretendia obter um resultado ordenado, a lista dos resultados foi clonada e efetuada uma operação de ordenação.
