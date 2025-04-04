import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

db_params = {
    "dbname": "star_comex_principal",
    "user": "postgres",
    "password": "postgres", 
    "host": os.getenv("DB_HOST"),
    "port": "5445",
    "options": "-c client_encoding=UTF8"
}

#Quais países mais exportam?
def obter_exportacoes_por_pais():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query = """
            SELECT p.nome AS pais, COALESCE(SUM(t.valor_monetario), 0) AS total_exportado
            FROM public.paises p
            LEFT JOIN public.transacoes t ON t.pais_origem = p.id
            LEFT JOIN public.tipos_transacoes tt ON t.tipo_id = tt.id AND tt.descricao = 'EXPORT'
            GROUP BY p.nome
            ORDER BY total_exportado DESC;
        """

        cursor.execute(query)
        resultado = cursor.fetchall()

        cursor.close()
        conn.close()

        return resultado

    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return []

#Quais produtos têm maior volume de exportação?
def obter_volume_exportacoes_por_produto():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query_exportacoes = """
            SELECT 
                p.descricao AS produto, 
                SUM(t.quantidade) AS volume_exportado
            FROM public.transacoes t
            JOIN public.produtos p ON t.produto_id = p.id
            JOIN public.tipos_transacoes tt ON t.tipo_id = tt.id
            WHERE tt.descricao = 'EXPORT'
            GROUP BY p.descricao
            ORDER BY volume_exportado DESC;
        """

        cursor.execute(query_exportacoes)
        resultado_exportacoes = cursor.fetchall()

        cursor.close()
        conn.close()

        return resultado_exportacoes

    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return []

#Quais produtos têm maior volume de importação?
def obter_volume_importacoes_por_produto():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query_importacoes = """
            SELECT 
                p.descricao AS produto, 
                SUM(t.quantidade) AS volume_importado
            FROM public.transacoes t
            JOIN public.produtos p ON t.produto_id = p.id
            JOIN public.tipos_transacoes tt ON t.tipo_id = tt.id
            WHERE tt.descricao = 'IMPORT'
            GROUP BY p.descricao
            ORDER BY volume_importado DESC;
        """

        cursor.execute(query_importacoes)
        resultado_importacoes = cursor.fetchall()

        cursor.close()
        conn.close()

        return resultado_importacoes

    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return []

#Qual a evolução do comércio por bloco econômico ao longo do tempo?
def obter_evolucao_comercio_por_bloco():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query = """
            SELECT
                bloco_economico,
                SUM(total_comercializado) AS total_por_bloco
            FROM (
                SELECT
                    b.nome AS bloco_economico,
                    SUM(t.valor_monetario) AS total_comercializado
                FROM public.transacoes t
                JOIN public.paises p ON t.pais_origem = p.id
                JOIN public.blocos_economicos b ON p.bloco_id = b.id
                JOIN public.cambios c ON t.cambio_id = c.id
                GROUP BY b.nome
            ) AS subconsulta
            GROUP BY bloco_economico
            ORDER BY total_por_bloco DESC;
        """

        cursor.execute(query)
        resultado = cursor.fetchall()

        cursor.close()
        conn.close()

        return resultado

    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return []

#Quais os principais parceiros comerciais de cada país?    
def obter_parceiros_comerciais():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query = """
            SELECT
                p_origem.nome AS pais,
                p_destino.nome AS parceiro_comercial,
                SUM(t.valor_monetario) AS total_comercializado
            FROM public.transacoes t
            JOIN public.paises p_origem ON t.pais_origem = p_origem.id
            JOIN public.paises p_destino ON t.pais_destino = p_destino.id
            GROUP BY p_origem.nome, p_destino.nome
            ORDER BY p_origem.nome, total_comercializado DESC;
        """

        cursor.execute(query)
        resultado = cursor.fetchall()

        cursor.close()
        conn.close()

        return resultado

    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return []

# Qual a variação das taxas de câmbio e seu impacto no comércio? exprotações
def obter_variacao_cambio_exportacoes():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query = """
            WITH Cambio_Anterior AS (
                SELECT 
                    c.id,
                    c.data,
                    c.moeda_origem,
                    c.moeda_destino,
                    c.taxa_cambio,
                    LAG(c.taxa_cambio) OVER (
                        PARTITION BY c.moeda_origem, c.moeda_destino ORDER BY c.data
                    ) AS taxa_cambio_anterior
                FROM public.cambios c
            )
            SELECT 
                t.id AS transacao_id,
                po.nome AS pais_origem,
                pd.nome AS pais_destino,
                p.descricao AS produto,
                tt.descricao AS tipo_transacao,
                mo_origem.descricao AS moeda_origem_nome,
                mo_destino.descricao AS moeda_destino_nome,
                c.data AS data_cambio,
                c.taxa_cambio,
                COALESCE(c.taxa_cambio_anterior, c.taxa_cambio) AS taxa_cambio_anterior,
                (c.taxa_cambio - COALESCE(c.taxa_cambio_anterior, c.taxa_cambio)) AS diferenca_variacao,
                t.valor_monetario AS valor_transacao,
                t.quantidade,
                (c.taxa_cambio - COALESCE(c.taxa_cambio_anterior, c.taxa_cambio)) * t.quantidade AS diferenca_valor
            FROM public.transacoes t
            JOIN Cambio_Anterior c ON t.cambio_id = c.id
            JOIN public.paises po ON t.pais_origem = po.id
            JOIN public.paises pd ON t.pais_destino = pd.id
            JOIN public.produtos p ON t.produto_id = p.id
            JOIN public.tipos_transacoes tt ON t.tipo_id = tt.id
            JOIN public.moedas mo_origem ON c.moeda_origem = mo_origem.id
            JOIN public.moedas mo_destino ON c.moeda_destino = mo_destino.id
            WHERE tt.descricao = 'EXPORT'
            ORDER BY c.data DESC;
        """

        cursor.execute(query)
        resultado = cursor.fetchall()

        cursor.close()
        conn.close()

        return resultado

    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return []
    
#Qual a variação das taxas de câmbio e seu impacto no comércio? importações
def obter_variacao_cambio_import():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query = """
            WITH Cambio_Anterior AS (
                SELECT 
                    c.id,
                    c.data,
                    c.moeda_origem,
                    c.moeda_destino,
                    c.taxa_cambio,
                    LAG(c.taxa_cambio) OVER (
                        PARTITION BY c.moeda_origem, c.moeda_destino ORDER BY c.data
                    ) AS taxa_cambio_anterior
                FROM public.cambios c
            )
            SELECT 
                t.id AS transacao_id,
                po.nome AS pais_origem,
                pd.nome AS pais_destino,
                p.descricao AS produto,
                tt.descricao AS tipo_transacao,
                mo_origem.descricao AS moeda_origem_nome,
                mo_destino.descricao AS moeda_destino_nome,
                c.data AS data_cambio,
                c.taxa_cambio,
                COALESCE(c.taxa_cambio_anterior, c.taxa_cambio) AS taxa_cambio_anterior,
                (c.taxa_cambio - COALESCE(c.taxa_cambio_anterior, c.taxa_cambio)) AS diferenca_variacao,
                t.valor_monetario AS valor_transacao,
                t.quantidade,
                (c.taxa_cambio - COALESCE(c.taxa_cambio_anterior, c.taxa_cambio)) * t.quantidade AS diferenca_valor
            FROM public.transacoes t
            JOIN Cambio_Anterior c ON t.cambio_id = c.id
            JOIN public.paises po ON t.pais_origem = po.id
            JOIN public.paises pd ON t.pais_destino = pd.id
            JOIN public.produtos p ON t.produto_id = p.id
            JOIN public.tipos_transacoes tt ON t.tipo_id = tt.id
            JOIN public.moedas mo_origem ON c.moeda_origem = mo_origem.id
            JOIN public.moedas mo_destino ON c.moeda_destino = mo_destino.id
            WHERE tt.descricao = 'IMPORT'
            ORDER BY c.data DESC;
        """

        cursor.execute(query)
        resultado = cursor.fetchall()

        cursor.close()
        conn.close()

        return resultado

    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return []
    
#Qual a distribuição dos meios de transporte utilizados nas transações?
def obter_percentual_transporte():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query = """
            SELECT 
                tr.descricao AS meio_transporte,
                COUNT(t.id) AS total_transacoes,
                ROUND((COUNT(t.id) * 100.0 / SUM(COUNT(t.id)) OVER ()), 2) AS percentual
            FROM public.transacoes t
            JOIN public.transportes tr ON t.transporte_id = tr.id
            GROUP BY tr.descricao
            ORDER BY total_transacoes DESC;
        """

        cursor.execute(query)
        resultado = cursor.fetchall()

        cursor.close()
        conn.close()

        return resultado

    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return []

#Qual valor total exportado por ano?
def obter_total_exportado_por_ano():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query = """
            SELECT 
                EXTRACT(YEAR FROM c.data) AS ano,
                SUM(t.valor_monetario) AS total_exportado
            FROM public.transacoes t
            JOIN public.tipos_transacoes tt ON t.tipo_id = tt.id
            JOIN public.cambios c ON t.cambio_id = c.id
            WHERE tt.descricao = 'EXPORT'
            GROUP BY ano
            ORDER BY ano DESC;
        """

        cursor.execute(query)
        resultado = cursor.fetchall()

        cursor.close()
        conn.close()

        return resultado

    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return []
    
#Qual valor total importado por ano?
def obter_total_importado_por_ano():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query = """
            SELECT 
                EXTRACT(YEAR FROM c.data) AS ano,
                SUM(t.valor_monetario) AS total_importado
            FROM public.transacoes t
            JOIN public.tipos_transacoes tt ON t.tipo_id = tt.id
            JOIN public.cambios c ON t.cambio_id = c.id
            WHERE tt.descricao = 'IMPORT'
            GROUP BY ano
            ORDER BY ano DESC;
        """

        cursor.execute(query)
        resultado = cursor.fetchall()

        cursor.close()
        conn.close()

        return resultado

    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return []