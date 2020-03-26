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
```

Por fim, a ideia para o cálculo dos colaboradores foi bastante semelhante. Tendo dois _inputs_ distintos, um do tipo __Filme Ator__ e outro __Filme Lista(Ator)__, usando _shuffle join_, é possível obter pares do tipo __Ator Lista(Ator)__. Esta lista é referente aos atores que participaram no mesmo filme do ator. Aplicando um _job_ para agrupar as chaves a este resultado, o ficheiro fica pronto a um _job_ de atualização da tabela. Note-se que neste é gerado um _Set_ por cada lista de colaboradores, uma vez que não se pretende observar repetidos, bem como aplicada uma condição em que se ignora o item da lista se ele for o próprio ator em estudo. Desta forma, como se pode ver em seguida, a coluna com informação dos colaboradores tem o aspeto pretendido.

```
get "actors_g4", "nm0000001"
COLUMN                                            CELL                                                                                                                                          
 Details:BirthYear                                timestamp=1584886788474, value=1899                                                                                                           
 Details:Collaborators                            timestamp=1585219565493, value=[nm0004847, nm0000049, nm0000765, nm0320352, nm0179376, nm0027323, nm0664990, nm0591486, nm2048327, nm0542554, 
                                                  nm0012196, nm0591485, nm0001417, nm0020537, nm3317570, nm0223290, nm2377837, nm0039838, nm0123611, nm2764227, nm0046331, nm0770196, nm0800808,
                                                   nm7812696, nm0028644, nm0242409, nm0053267, nm0000859, nm0780750, nm0892143, nm0295044, nm0675273, nm0361875, nm0076282, nm0549359, nm0005464
                                                  , nm0006752, nm0739347, nm0122675, nm0122875, nm0000742, nm1126630, nm0001677, nm1451334, nm0000267, nm0068691, nm0000268, nm0039051, nm000131
                                                  3, nm0177731, nm0081748, nm0427775, nm0133310, nm0000781, nm0931868, nm7277740, nm0385807, nm0020239, nm0765109, nm0000748, nm7277741, nm01379
                                                  99, nm0662955, nm7277739, nm0008066, nm0200488, nm0040644, nm0016776, nm0578459, nm0039938, nm0050639, nm0026802, nm0428278, nm1157487, nm0017
                                                  651, nm0016361, nm0614500, nm0015391, nm1229000, nm0388913, nm0713073, nm0771349, nm0382295, nm0188050, nm0114089, nm0492747, nm0001362, nm003
                                                  0019, nm0649701]                                                                                                                              
 Details:DeathYear                                timestamp=1584886788474, value=1987                                                                                                           
 Details:PrimaryName                              timestamp=1584886788474, value=Fred Astaire                                                                                                   
 Details:Top3Movies                               timestamp=1585137315959, value=[tt1298866, tt0816598, tt0034409]                                                                              
 Details:TotalMovies                              timestamp=1584888960577, value=\x00\x00\x00\x00\x00\x00\x01'                                                                                  
6 row(s) in 0.0310 seconds


