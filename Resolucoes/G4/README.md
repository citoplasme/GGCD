# Guião Número 4 de GGCD

### HBase + MapReduce

##### Exercício 1

O exercício 1 consistiu em efetuar dois _jobs_, um primeiro para carregar detalhes como nome do filme e um segundo alusivo aos _ratings_ do mesmo, uma vez que estes dados se encontravam noutro ficheiro. Note-se que poderiam ser passados ambos os ficheiros ao _job_, mas, de forma a permitir uma leitura simples do código, foi optada pela sua separação. 
Após a execução, se for efetuado um pedido de __GET__ por um determinado registo, este será o seu aspeto:

```
$ get 'movies', "tt0000001"
COLUMN                CELL                                                      
 Details:EndYear      timestamp=1584126093154, value=\x5CN                      
 Details:Genres       timestamp=1584126093154, value=Documentary,Short          
 Details:OriginalTitl timestamp=1584126093154, value=Carmencita                 
 e                                                                              
 Details:PrimaryTitle timestamp=1584126093154, value=Carmencita                 
 Details:Rating       timestamp=1584890118240, value=5.6                        
 Details:Runtime      timestamp=1584126093154, value=1                          
 Details:StartYear    timestamp=1584126093154, value=1894                       
 Details:TitleType    timestamp=1584126093154, value=short                      
 Details:isAdult      timestamp=1584126093154, value=0                          
9 row(s) in 0.0320 seconds
```

##### Exercício 2

No que toca ao povoamento dos atores, esta tarefa mostrou-se bastante mais complexa, uma vez que implicou a uma maior separação dos passos para obter o resultado pretendido. Campos como o nome, nascimento e morte foram carregados diretamente a partir dos dados fornecidos. Já para o cálculo do número de filmes por ator, foi necessário efetuar um primeiro _job_ que agrupasse filmes por ator. Quer isto dizer que cada linha gerada no final deste primeiro _job_ teria o aspeto: ator1 filme1 filme2 ... .

Após o primeiro _job_ os registos da tabela possuem o seguinte aspeto:
```
$ get "actors_g4", "nm0048901"
COLUMN                CELL                                                      
 Details:BirthYear    timestamp=1584886790326, value=\x5CN                      
 Details:DeathYear    timestamp=1584886790326, value=\x5CN                      
 Details:PrimaryName  timestamp=1584886790326, value=Ryan Baker                 
3 row(s) in 0.1410 seconds
```

Executando um job ao resultado obtido, apenas é necessário contar o número de campos separados por um ou mais _tabs_, sendo retirado um, referente ao identificador do ator. Tendo-se na tabela registos semelhantes ao seguinte:

```
$ get "actors_g4", "nm0048903"
COLUMN                CELL                                                      
 Details:BirthYear    timestamp=1584886790326, value=\x5CN                      
 Details:DeathYear    timestamp=1584886790326, value=\x5CN                      
 Details:PrimaryName  timestamp=1584886790326, value=Said Baker                 
 Details:TotalMovies  timestamp=1584888961626, value=\x00\x00\x00\x00\x00\x00\x0
                      0\x03                                                     
4 row(s) in 0.0370 seconds
```

No que toca ao cálculo do _top_ 3 filmes do ator, sendo este _top_ baseado nos _ratings_ dos filmes, a ideia passou por começar com um _job_ que tirasse proveito de _shuffle join_. Para isso, o _job_ recebe como argumento dois ficheiros, um com o identificador do filme e o respetivo _rating_ e outro com o identificador do ator associado à listagem de filmes em que participou. A ideia é gerar contextos do tipo: __filme R+rating__ e __filme L+ator__. Depois, no _reducer_ é gerado um contexto do tipo __ator filme#rating__, permitindo que, com outro job, se associe por cada ator uma listagem dos filmes em que participou, com os respetivos _ratings_. No final, correndo-se outro _job_, apenas se calcula a lista dos 3 filmes melhor cotados associados àquela entrada no ficheiro gerado pelo _job_ anterior. Note-se que inicialmente a ideia passava por efetuar _GETS_ à base de dados para a ordenação dos filmes por _rating_, mas a solução mostrou-se demasiado lenta.
O resultado deste _job_ pode ser visível em seguida.

```
get "actors_g4", "nm0000001"
COLUMN                CELL                                                      
 Details:BirthYear    timestamp=1584886788474, value=1899                       
 Details:DeathYear    timestamp=1584886788474, value=1987                       
 Details:PrimaryName  timestamp=1584886788474, value=Fred Astaire               
 Details:Top3Movies   timestamp=1585137315959, value=[tt1298866, tt0816598, tt00
                      34409]                                                    
 Details:TotalMovies  timestamp=1584888960577, value=\x00\x00\x00\x00\x00\x00\x0
                      1'                                                        
5 row(s) in 0.0790 seconds




