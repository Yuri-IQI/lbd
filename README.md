# Projeto de Laboratório de Banco de Dados

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



O arquivo .env está configurado para executar expor o banco na 5445, para mudar a porta, apenas mude o valor da variável DB_PORT.
