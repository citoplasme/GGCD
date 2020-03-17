# Guião Número 3 de GGCD

### HBase

##### Exercício 1

O exercício 1 consistiu em gerar dados de filmes e respetivos votos, ou seja, a um respetivo identificador fica associada uma lista de votos ([0,10]).
Para isso, além de gerar os dados, foi necessário criar a tabela numa primeira fase.
Em seguida, na fase de __PUT__, como se trata de atualização de uma lista, é necessário efetuar um __GET__ pela lista dos votos associados ao filme em questão. Caso não exista, é criada uma nova em que se adiciona o novo voto. Em caso de já existir, apenas se adiciona o novo voto e atualiza o registo.

##### Exercício 2

Tendo em conta a resolução implementada para o exercício 1, o exercício 2 já se encontrava resolvido, quer para a adição de um só voto, como de vários. Em ambos os casos, foi notória a velocidade de resposta por parte do _HBase_ na atualização da lista.

##### Exercício 3

Este consistiu numa ação bastante semelhante ao primeiro exercício, em que se criou uma tabela e, em seguida, se efetuou o povoamento da mesma. Tendo em conta que o objetivo era a criação de registos com vários campos, na tabela _HBase_ são geradas __N__ linhas, sendo __N__ o número de atributos distintos.

##### Exercício 4

Por fim, o exercício 4 pretendia a execução de um __SCAN__ completo à base de dados. Note-se que, de modo a apresentar os dados da forma mais legível possível, os _bytes_ de cada valor de atributos devem ser convertidos num tipo legível (_String_). É de destacar que como estes dados poderiam ser utilizados para servir uma aplicação em tempo real, por exemplo _web_, a impressão dos mesmos foi efetuada num formato _json-like_.
