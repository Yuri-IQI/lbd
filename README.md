# Acesso ao Banco

## Ambiente Local

### Utilizando Docker

Para subir o container do banco e do ETL apenas execute o seguinte comando na pasta raís:
```bash
docker compose up -d
```

Após subir o container, verificar se o banco está rodando com o seguinte comando:
```bash
docker ps
```

O container do banco deverá ser listado no terminal após a execucação do comando, quanto ao container do ETL, ele deverá apenas executar o job e parar, portanto ele pode não estar visível na lista.
Se o container do spark estiver listado ele ainda não finalizou o job.

Para verificar se o job foi executado faça os seguintes passos:
1. Entre no container do banco: \
`docker exec -it postgres bash`

3. Entre no banco com: \
`psql -U postgres`

5. Entre no banco do data mart com: \
`\c star_comex_data_mart`

7. Faça um select na tabela fatos para saber se o ETL foi executado: \
`select * from ft_transacoes;`

8. saia do banco e do container com o comando:
`exit`

Caso nenhuma informação tenha sido retornada pela consulta, verifique se ocorreu algum erro no container do spark:
```bash
docker logs spark_job
```

## Ambiente da VM

### Conectando ao Banco da VM

Na VM, existe um container postgres sendo executado contendo o banco principal e o data mart. 

Para se conectar ao banco na vm, consulte os métodos de conexão no [colab](https://colab.research.google.com/drive/1viZIOcaQYkDnhfeNsdSMdQzNU8VktmWr?usp=sharing) e use o mesmo usuário e senha do ETL.

# Explicação do ETL

## Funções Gerais
No começo do código do ETL, são declaradas algumas funções que generalizam o processo de ETL e são comuns no tratamento da maioria das tabelas.

**extract_from_principal**
> Essa função é usada para realizar a extração de dados do banco principal, ela utiliza uma consulta para gerar um dataframe com base nos resultados.

**transform_text_to_column**
> Essa é uma função de tratamento aplicada a todos os campos de texto do banco, ela recebe um dataframe e uma lista de colunas e então ela itera sobre as colunas da lista e passa o texto para maiusculo.

**load_to_data_mart**
> Essa é a função de carga para o data mart, ela recebe o dataframe com os dados tratados e o nome da tabela no banco do data mart em que os dados do dataframe devem ser inseridos.

**add_surrogate_key**
> Essa função é usada para criar uma surrogate key que será uma chave primária própria do data mart, ela recebe um dataframe e o nome da coluna de chave primaria no banco principal com isso ela cria e nomeia a surrogate key com base no nome e valor da chave natural.

**get_currency_from_country_code**
> No banco principal, exceto por uma entrada, a tabela de moedas não utiliza o nome das moedas dos paises, ao invés disso, ela utiliza as siglas dos paises. Porém a API frankfurter só permite a busca utilizando as moedas, então essa função utiliza as bibliotecas babel e pycountry para converter a sigla do país para o nome da moeda.

**etl_products**
> Essa é a função que realiza o processo de ETL para a dimensão produtos, ela apenas extrai os dados dos produtos e das categorias, tranformas os campos de texto para maiusculo e cria uma sk antes de fazer a carga no data mart.

**etl_transports**
> Essa função segue o mesmo padrão de etl_products para fazer o etl da dimensão de transportes.

**etl_countries**
> Essa função segue o mesmo padrão da etl_products e da etl_transports para fazer o etl para a dimensão paises.

# 死にたい
