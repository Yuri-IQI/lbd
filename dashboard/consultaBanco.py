import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

db_params = {
    "dbname": "star_comex_data_mart",
    "user": "postgres",
    "password": "postgres", 
    "host": os.getenv("DB_HOST"),
    "port": "5445",
    "options": "-c client_encoding=UTF8"
}

# Quais países mais exportam?
def obter_exportacoes_por_pais():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query = """
            SELECT dp.pais, COALESCE(SUM(ft.valor_monetario), 0) AS total_exportado
            FROM ft_transacoes ft
            INNER JOIN dm_pais dp on dp.sk_pais = ft.sk_pais_origem
            WHERE ft.tp_transacao = 'EXPORT'
            GROUP BY dp.pais
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

# Quais produtos têm maior volume de exportação?
def obter_volume_exportacoes_por_produto():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query_exportacoes = """
            SELECT
                dp.descricao AS produto,
                SUM(ft.quantidade) AS volume_exportado
            FROM ft_transacoes ft
            JOIN dm_produtos dp ON ft.sk_produto = dp.sk_produto
            WHERE ft.tp_transacao = 'EXPORT'
            GROUP BY 1
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

# Quais produtos têm maior volume de importação?
def obter_volume_importacoes_por_produto():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query_importacoes = """
            SELECT
                dp.descricao AS produto,
                SUM(ft.quantidade) AS volume_exportado
            FROM ft_transacoes ft
            JOIN dm_produtos dp ON ft.sk_produto = dp.sk_produto
            WHERE ft.tp_transacao = 'IMPORT'
            GROUP BY 1
            ORDER BY volume_exportado DESC;
        """

        cursor.execute(query_importacoes)
        resultado_importacoes = cursor.fetchall()

        cursor.close()
        conn.close()

        return resultado_importacoes

    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return []

# Qual a evolução do comércio por bloco econômico ao longo do tempo?
def obter_evolucao_comercio_por_bloco():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query = """
            SELECT 
                dp1.nm_bloco AS bloco_origem,
                dp2.nm_bloco AS bloco_destino,
                dt.ano,
                dt.mes,
                dt.dia,
                ft.tp_transacao,
                SUM(ft.valor_monetario) AS total_valor_monetario
            FROM 
                ft_transacoes ft
            INNER JOIN 
                dm_pais dp1 ON dp1.sk_pais = ft.sk_pais_origem
            INNER JOIN 
                dm_pais dp2 ON dp2.sk_pais = ft.sk_pais_destino
            INNER JOIN 
                dm_tempo dt ON dt.sk_tempo = ft.sk_tempo
            GROUP BY 
                dp1.nm_bloco, dp2.nm_bloco, dt.ano, dt.mes, dt.dia, ft.tp_transacao
            ORDER BY 
                dt.ano, dt.mes, dt.dia, dp1.nm_bloco, dp2.nm_bloco;
        """

        cursor.execute(query)
        resultado = cursor.fetchall()

        cursor.close()
        conn.close()

        return resultado

    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return []

# Quais os principais parceiros comerciais de cada país?    
def obter_parceiros_comerciais():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query = """
            SELECT
                p1.pais AS pais,
                p2.pais AS parceiro_comercial,
                SUM(ft.valor_monetario) AS total_comercializado,
                ft.tp_transacao
            FROM ft_transacoes ft
            JOIN dm_pais p1 ON ft.sk_pais_origem = p1.sk_pais
            JOIN dm_pais p2 ON ft.sk_pais_destino = p2.sk_pais
            GROUP BY p1.pais, p2.pais, ft.tp_transacao
            ORDER BY p1.pais, total_comercializado DESC;
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
            SELECT dc.cd_moeda_origem, dc.cd_moeda_destino, dc.taxa_cambio,
                (SELECT SUM(ft2.valor_monetario)
                        FROM ft_transacoes ft2
                        INNER JOIN dm_tempo dt2 ON ft2.sk_tempo = dt2.sk_tempo
                        WHERE ft.tp_transacao = ft2.tp_transacao AND dt2.data_completa = dt.data_completa) as total,
                dt.ano, dt.mes, dt.dia, ft.tp_transacao
            FROM ft_transacoes ft
            INNER JOIN dm_cambios dc on dc.sk_cambio = ft.sk_cambio
            INNER JOIN dm_tempo dt on dt.sk_tempo = ft.sk_tempo
            GROUP BY dc.cd_moeda_origem, dc.cd_moeda_destino, dc.taxa_cambio, dt.ano, dt.mes, dt.dia, ft.tp_transacao, dt.data_completa
            ORDER BY ano, mes, dia
        """

        cursor.execute(query)
        resultado = cursor.fetchall()

        cursor.close()
        conn.close()

        return resultado

    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return []
    
# Qual a distribuição dos meios de transporte utilizados nas transações?
def obter_percentual_transporte():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query = """
            SELECT
                dt.ds_transporte AS meio_transporte,
                COUNT(ft.sk_transporte) AS total_transacoes,
                ROUND((COUNT(ft.sk_transporte) * 100.0 / SUM(COUNT(ft.sk_transporte)) OVER ()), 2) AS percentual
            FROM ft_transacoes ft
            JOIN dm_transporte dt ON ft.sk_transporte = dt.sk_transporte
            GROUP BY dt.ds_transporte
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

# Qual valor total exportado por ano?
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
    
# Qual valor total importado por ano?
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
