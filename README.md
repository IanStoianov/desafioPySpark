#Case InBEv
#Autor: Ian Stoianov Loureiro Arthur


#Guia para execução:

1 - Criar um volume no docker com o seguinte comando:
docker volume create postgres_volume

2 - Subir o postgres usando imagem oficial do docker, com as seguintes configurações
docker run --name=postgresdb -e POSTGRES_PASSWORD=secret -p 5499:5432 -d -v postgres_volume:/var/lib/postgresql/data postgres

3.1 (opcional) - Conectar no postgres pelo pgAdmin com as seguinte configurações:

host: host.docker.internal 
porta: 5499
usuário: postgres
senha: secret

Em seguida executar o script de criação de tabela providenciado

3.2 (opcional) - Ou direto no terminal com o comando:
docker exec -ti db psql -U postgres

Em seguida executar o script de criação de tabela providenciado
Verificar com o comando \dt se a tabela foi criada


4 - Cadastrar a DAG no Airflow
  - Subir a pasta do projeto, a DAG já leva em conta a existência do módulo do código
  - Garantir a presença das libs requests, pyspark, e que o Hadoop esteja presente e configurado
    - Versões usadas:
      Python 3.11.9
      pyspark 3.5.3
      Hadoop 3.3.6

5 - Executar a DAG




#Considerações:
- A DAG foi executada em modo local, utilizando uma instância do Ubuntu executada via WSL2 dentro de ambiente Windows. Como possuía total controle do ambiente virtualizado, consegui garantir que todas dependências fossem cumpridas.
  No entanto, tive problemas para garantir todas as dependências utilizando o Airflow via docker. Arquivos docker-compose.yml e Dockerfile PARCIAIS foram disponibilizados

- O script pode ser executado de forma isolada em ambiente Windows ou Unix, utiliza tratamento para garantir o caminho de arquivos carregados e salvos de acordo com o sistema operacinal

- Adicionei tratamento de erros, mas fora erros habituais durante o desenvolvimento, como o path de algum componente não estar configurado corretamente, uma vez que o script passou a executar com sucesso, não encontrei mais erros de execução.
  No entanto, nos pontos que considerei mais críticos há tratamento de erro no código, e levantam mensagem apontando o que ocorreu, caso algo venha a acontecer.
    - Visando garantir que não ocorrese problemas durante consultas à API, é efetuado somente uma consulta por segundo

- A aplicação tem um volume de dados relativamente baixo, abaixo de 9000 entradas quando do desenvolvimento. O uso das ferramentas utilizadas é adequado para este volume, mas seria interssante avaliar ferramentas mais poderosas caso o volume fosse maior.
  Por exemplo, para processo na escala de milhões de entradas, uma carga diária, ou de várias vezes ao dia, poderia se beneficiar de utilizar clusters EMR para paralelização do processamento de dados. Nesse caso, a orquestração poderia ser através de AWS Step Functions, que podem gerenciar a subida do cluster, execução do código, e desligamento do cluster

- Analisando os dados carregados da API, não identifiquei nenhuma inconsistência. Nomes de países, tipos de local, etc, todos apresentavam consistência, sem dados fora do esperado
  No entando, identifiquei duplicação de algumas entradas, inconsistência que foi devidamente tratada. Nenhuma outra "inconsistência", mesmo presença de campos vazios, destacou como algo que devesse ser tratato, e sim como o dado disponível na fonte.
  Outros tipos de uso dos dados poderiam justificar um tratamento desses campos vazios, mas para o propósito desse desafio, não era necessário para os o que foi utilizado

- Dados apresentam consintentes de uma carga completa para outra, trazendo inclusive a mesma ordem de apresentação. 
  Como não é possível garantir que eventuais atualizações dos dados disponíveis na API não alterem a ordem de apresentação, somado com a falta de campo de data, julguei mais prudente efetuar uma carga completa a cada execução, sem a consideração de quais dados já estivessem disponíveis
  - Posteriormente, caso se identifique que a ordem é respeitada quando o dados sofrem atualização na API, pode-se implementar um ajuste para trazer somen

- Apesar de ser apresentado o script de criação de tabela, não é estritamente necessária a execução prévia do mesmo, pois o banco é criado pelo comando de inserção no banco durante uma primeira execução
  - Tipos de cada coluna foi definido como texto, formato que é mantido quando a criação é feita automaticamente. Julguei mais prudente manter os campos como texto para preservar a estrutra original dos JSONs capturados através da API
    Caso fosse efetuada a carga para outras tabelas relacionais, representando as camadas 'silver' e 'gold', seria interessante definir os tipos das colunas para utlização subsequente

- Os dados parquet salvos podem ser lidos como um datalake e, se salvos dentro de bucket AWS S3, podem ser consultados via AWS Athena utilizando um catálogo AWS Glue, por exemplo.

- O código poderia ser executado em somente uma etapa caso caso fosse deseajado, visto que os dados salvos por uma etapa são lidos na etapa subsequente. Isso, no entanto, diminuiria a compartimentalização do código e não seria interessante para processos com maior volume de dados. 