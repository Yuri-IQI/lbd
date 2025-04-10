# Dashboard
http://ec2-13-218-45-115.compute-1.amazonaws.com:8501

# Acesso ao Banco

## Ambiente Local

### Utilizando Docker

Para subir os containers do banco de dados e do ETL, execute o seguinte comando na pasta raiz do projeto:

```bash
docker compose up -d
```

Após subir os containers, verifique se o banco está rodando com:

```bash
docker ps
```

O container do banco deve aparecer listado no terminal.  
O container do ETL executa o job e finaliza em seguida, portanto ele **pode não aparecer** na lista após a execução.

Se o container do Spark ainda estiver listado, é porque o job ainda está em execução.

---

### Verificando a Execução do Job ETL

1. Acesse o container do banco de dados:
   ```bash
   docker exec -it postgres bash
   ```

2. Acesse o PostgreSQL:
   ```bash
   psql -U postgres
   ```

3. Conecte-se ao banco do Data Mart:
   ```sql
   \c star_comex_data_mart
   ```

4. Verifique se há registros na tabela de fatos:
   ```sql
   SELECT * FROM ft_transacoes;
   ```

5. Saia do banco e do container com:
   ```bash
   exit
   ```

Se nenhuma informação for retornada, verifique os logs do container Spark:

```bash
docker logs spark_job
```

---

## Ambiente da VM

### Conectando ao Banco da VM

Na máquina virtual (VM), um container PostgreSQL está em execução contendo tanto o **banco principal** quanto o **Data Mart**.

Para se conectar ao banco da VM:

- Acesse os métodos de conexão descritos neste [notebook do Colab](https://colab.research.google.com/drive/1viZIOcaQYkDnhfeNsdSMdQzNU8VktmWr?usp=sharing)
- Utilize as mesmas **credenciais de usuário e senha** utilizadas pelo ETL

## Explicação do ETL

Abaixo estão descritas as funções que compõem o pipeline de ETL, organizadas conforme sua responsabilidade no processo de extração, transformação e carga dos dados.

---

### 🔹 `extract_from_principal()`

Realiza a **extração de dados do banco principal** a partir de uma consulta SQL.  
Retorna os resultados da query como um DataFrame do Spark.

---

### 🔹 `transform_text_to_column(df, columns: list)`

Aplica **tratamento padronizado em colunas de texto**.  
Recebe um DataFrame e uma lista de colunas e converte o conteúdo dessas colunas para **maiúsculas**.

---

### 🔹 `load_to_data_mart(df, table_name: str)`

Responsável por realizar a **carga de dados no Data Mart**.  
Recebe um DataFrame tratado e o nome da tabela destino no Data Mart, e realiza a inserção dos dados.

---

### 🔹 `add_surrogate_key(df, natural_key_column: str)`

Gera uma **chave substituta (surrogate key)** com base na chave natural.  
É utilizada para criar uma nova chave primária interna ao Data Mart, mantendo a integridade das dimensões.

---

### 🔹 `get_currency_from_country_code(country_code: str)`

Converte **códigos de países (siglas ISO)** para **nomes de moedas**.  
Isso é necessário pois a API [Frankfurter](https://www.frankfurter.app) aceita apenas nomes de moedas como parâmetro.  
A função utiliza as bibliotecas `babel` e `pycountry` para realizar a conversão.

---

### 🔹 `etl_products()`

Executa o processo de ETL para a **dimensão produtos**:
- Extrai dados de produtos e categorias
- Aplica transformação em colunas de texto
- Gera surrogate keys
- Realiza a carga no Data Mart

---

### 🔹 `etl_transports()`

Executa o processo de ETL para a **dimensão transportes**, seguindo a mesma estrutura da função `etl_products`.

---

### 🔹 `etl_countries()`

Executa o processo de ETL para a **dimensão países**, também utilizando a mesma abordagem usada nas dimensões anteriores.

---

### 🔹 `etl_exchange_rates()`

Realiza o ETL da **dimensão de câmbio**:
- Substitui as taxas de câmbio do banco principal por taxas obtidas via API Frankfurter.
- Utiliza a função `fetch_exchange_rate()` passando a data e os países envolvidos para obter a taxa atualizada.

---

### 🔹 `fetch_exchange_rate(date, from_country_code, to_country)`

Obtém a taxa de câmbio entre dois países em uma data específica:  
- Converte os códigos dos países em moedas utilizando `get_currency_from_country_code`
- Consulta a API Frankfurter para retornar a taxa de câmbio correta

---

### 🔹 `etl_time_from_exchange(exchange_df)`

Recebe o DataFrame com as taxas de câmbio e extrai a **dimensão temporal**, com base nas datas das transações de câmbio.

---

### 🔹 `etl_facts()`

Executa o ETL da **tabela fato**, unificando os dados:
- Extrai as transações do banco principal
- Realiza joins com todas as dimensões previamente criadas
- Prepara os dados para análise no Data Mart

---
