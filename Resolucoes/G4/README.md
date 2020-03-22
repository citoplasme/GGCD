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
Executando um job ao resultado obtido, apenas é necessário contar o número de campos separados por um ou mais _tabs_, sendo retirado um, referente ao identificador do ator.


