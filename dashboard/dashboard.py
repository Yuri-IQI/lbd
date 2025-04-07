import streamlit as st
import consultaBanco
import plotly.graph_objects as go
import plotly.express as px
from collections import defaultdict
from datetime import date, datetime

st.set_page_config(page_title="Dashboard Comércio Exterior", layout="wide")
# Detecta tema
tema_atual = st.get_option("theme.base")
modo_escuro = tema_atual == "dark"
template_plotly = "plotly_dark" if modo_escuro else "plotly_white"
cor_texto_titulo = "#FFFFFF" if modo_escuro else "#000000"

st.markdown("""
    <h1 style='
        font-size: 52px;
        font-weight: bold;
         text-align: center;              
        font-family: "Segoe UI", sans-serif;
        margin-bottom: 10px;
    '>
        🌎 Dashboard StarComex
    </h1>
""", unsafe_allow_html=True)

st.sidebar.markdown("""
    <h1 style='font-size: 29px; font-weight: 600; margin-bottom: -50px;'>
        📂 Escolha uma opção
    </h3>
""", unsafe_allow_html=True)

aba = st.sidebar.selectbox(
    "If I must",
    [
        "Exportações por País",
        "Volume de Importação/Exportação por Produto",
        "Valor Total Exportado por Ano",
        "Valor Total Importado por Ano",
        "Comércio por Bloco Econômico",
        "Parceiros Comerciais",
        "Variação Câmbial",
        "Distribuição por Meio de Transporte"
    ],
    label_visibility="hidden"
)
st.sidebar.image("logo.png", use_container_width=True)

def grafico_barras(titulo, categorias, valores, x_title, y_title):
    cores = px.colors.qualitative.Plotly
    cores_usadas = [cores[i % len(cores)] for i in range(len(categorias))]

    fig = go.Figure(data=[go.Bar(
        x=categorias,
        y=valores,
        marker=dict(color=cores_usadas)
    )])

    fig.update_layout(
        title=dict(text=titulo, font=dict(size=22)),
        xaxis_title=x_title,
        yaxis_title=y_title,
        template="plotly_white",
        margin=dict(l=40, r=40, t=80, b=60),
        height=500,
        font=dict(size=14),
        legend_title=dict(text="Categorias", font=dict(size=16))
    )

    st.plotly_chart(fig, use_container_width=True)

def grafico_colunas(titulo, categorias, valores, x_title, y_title):
    fig = go.Figure(data=[go.Bar(
        x=categorias,
        y=valores,
        marker=dict(color=px.colors.qualitative.Plotly),
        text=[f"{v:,.0f}" for v in valores],
        textposition='auto'
    )])

    fig.update_layout(
        title=dict(text=titulo, font=dict(size=22)),
        xaxis_title=x_title,
        yaxis_title=y_title,
        template="plotly_white",
        margin=dict(l=40, r=40, t=80, b=60),
        height=500,
        font=dict(size=14),
        yaxis_tickprefix="$",
        yaxis_tickformat=",.0f"
    )

    st.plotly_chart(fig, use_container_width=True)

def grafico_linha(titulo, categorias, valores, x_title, y_title):
    fig = go.Figure(data=go.Scatter(x=categorias, y=valores, mode='lines+markers', line=dict(color='green')))
    fig.update_layout(
        title=titulo,
        xaxis_title=x_title,
        yaxis_title=y_title,
        template="plotly_white",
        margin=dict(l=40, r=40, t=80, b=40),
        height=500
    )
    st.plotly_chart(fig, use_container_width=True)

def grafico_pizza(titulo, categorias, valores):
    fig = go.Figure(data=[go.Pie(
        labels=categorias,
        values=valores,
        hole=0.4,
        textinfo='label+percent',
        textfont=dict(size=16),
        marker=dict(line=dict(color='#000000', width=2))
    )])

    fig.update_layout(
        title=titulo,
        title_font=dict(size=24),
        legend_title="Categorias",
        legend=dict(
            font=dict(size=18),
            orientation="v",
            x=1,
            y=0.5
        ),
        margin=dict(l=20, r=20, t=80, b=20),
        height=500
    )

    st.plotly_chart(fig, use_container_width=True)

def grafico_barras_duplas(titulo, dataset, left_bar, right_bar):
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=dataset,
        y=left_bar,
        name="Importações",
        marker_color='#FF9878'
    ))

    fig.add_trace(go.Bar(
        x=dataset,
        y=right_bar,
        name="Exportações",
        marker_color='#C8FF78'
    ))

    fig.update_layout(
        title=titulo,
        xaxis_title="Produto",
        yaxis_title="Volume",
        barmode='group',
        template="plotly_white",
        margin=dict(l=40, r=40, t=80, b=60),
        height=500,
        font=dict(size=14)
    )

    st.plotly_chart(fig, use_container_width=True)

# Exportações por País
if aba == "Exportações por País":
    dados = consultaBanco.obter_exportacoes_por_pais()
    if dados:
        paises = [linha[0] for linha in dados]
        valores = [linha[1] for linha in dados]
        grafico_colunas("🌍 Exportações por País", paises, valores, "País", "Total Exportado")
    else:
        st.warning("Nenhum dado encontrado.")

# Volume de Importação/Exportação por Produto
elif aba == "Volume de Importação/Exportação por Produto":
    dados_import = consultaBanco.obter_volume_importacoes_por_produto()
    dados_export = consultaBanco.obter_volume_exportacoes_por_produto()

    if dados_import and dados_export:
        produtos_import = [linha[0] for linha in dados_import]
        volumes_import = [linha[1] for linha in dados_import]

        produtos_export = [linha[0] for linha in dados_export]
        volumes_export = [linha[1] for linha in dados_export]

        produtos_combinados = list(set(produtos_import) | set(produtos_export))
        produtos_combinados.sort()

        dict_import = dict(dados_import)
        dict_export = dict(dados_export)

        volumes_import_final = [dict_import.get(produto, 0) for produto in produtos_combinados]
        volumes_export_final = [dict_export.get(produto, 0) for produto in produtos_combinados]

        grafico_barras_duplas("📦 Volume de Comércio por Produto", produtos_combinados, volumes_import, volumes_export)
    else:
        st.warning("Nenhum dado encontrado.")

# Valor Total Exportado por Ano
elif aba == "Valor Total Exportado por Ano":
    dados = consultaBanco.obter_total_exportado_por_ano()
    if dados:
        anos = [int(linha[0]) for linha in dados]
        totais = [linha[1] for linha in dados]
        grafico_colunas("📊 Valor Total Exportado por Ano", anos, totais, "Ano", "Valor Total Exportado")
    else:
        st.warning("Nenhum dado encontrado.")

# Total Importado por Ano
elif aba == "Valor Total Importado por Ano":
    dados = consultaBanco.obter_total_importado_por_ano()
    if dados:
        anos = [int(linha[0]) for linha in dados]
        totais = [linha[1] for linha in dados]
        grafico_colunas("📉 Total Importado por Ano", anos, totais, "Ano", "Total Importado")
    else:
        st.warning("Nenhum dado encontrado.")


# Comércio por Bloco Econômico
elif aba == "Comércio por Bloco Econômico":
    dados = consultaBanco.obter_evolucao_comercio_por_bloco()
    
    if dados:
        tipos_map = {"Importações": "IMPORT", "Exportações": "EXPORT"}

        opcoes = list(tipos_map.keys())
        tipo_escolhido = st.radio("Selecione o tipo de transação", opcoes)
        
        tipo_valor = tipos_map.get(tipo_escolhido)

        blocos_por_data = defaultdict(list)

        for linha in dados:
            bloco_origem = linha[0]
            ano, mes, dia = linha[2], linha[3], linha[4]
            tp_transacao = linha[5]
            valor = linha[6]
            
            if tp_transacao == tipo_valor:
                data = date(ano, mes, dia)
                blocos_por_data[bloco_origem].append((data, valor))

        fig = go.Figure()
        for bloco, entradas in blocos_por_data.items():
            entradas.sort() 
            datas = [item[0] for item in entradas]
            valores = [item[1] for item in entradas]

            fig.add_trace(go.Scatter(
                x=datas,
                y=valores,
                mode="lines+markers",
                name=bloco
            ))

        fig.update_layout(
            title=f"📈 Evolução do Comércio por Bloco Econômico - {tipo_valor.title()}ações",
            xaxis_title="Data",
            yaxis_title="Valor Monetário Total",
            template="plotly_white",
            height=500
        )

        st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.warning("Nenhum dado encontrado.")

# Parceiros Comerciais (versão melhorada)
elif aba == "Parceiros Comerciais":
    dados = consultaBanco.obter_parceiros_comerciais()
    st.subheader("📊 Parceiros Comerciais")

    if dados:
        paises_origem = sorted(set([linha[0] for linha in dados]))
        paises_map = {pais.title(): pais for pais in paises_origem}

        pais_opcoes = list(paises_map.keys())
        pais_escolhido_title = st.radio("Selecione o país de origem:", pais_opcoes, horizontal=True)
        pais_escolhido = paises_map[pais_escolhido_title]

        tipos_map = {"Importações": "IMPORT", "Exportações": "EXPORT"}

        tipos_opcoes = list(tipos_map.keys())
        tipo_escolhido = st.radio("Selecione o tipo de transação", tipos_opcoes, horizontal=True)
        
        tipo_valor = tipos_map.get(tipo_escolhido)

        parceiros = [linha[1] for linha in dados if linha[0] == pais_escolhido]
        valores = [linha[2] for linha in dados if linha[0] == pais_escolhido and linha[3] == tipo_valor]

        if parceiros and valores:
            grafico_pizza(f"🌍 Distribuição de Parceiros Comerciais - {pais_escolhido_title}", parceiros, valores)
        else:
            st.warning("Nenhum dado de parceiros encontrado para este país.")
    else:
        st.warning("Nenhum dado encontrado.")

# Variação Câmbio - Exportações
elif aba == "Variação Câmbial":
    dados = consultaBanco.obter_variacao_cambio_exportacoes()
    if dados:

        tipos_map = {"Importações": "IMPORT", "Exportações": "EXPORT"}

        tipos_opcoes = list(tipos_map.keys())
        tipo_escolhido = st.radio("Selecione o tipo de transação", tipos_opcoes, horizontal=True)
        
        tipo_valor = tipos_map.get(tipo_escolhido)

        moedas_origem = [linha[0] for linha in dados]
        moedas_destino = [linha[1] for linha in dados]

        moeda_origem = st.selectbox(
            "Selecione a moeda de origem:",
            options=sorted(set(moedas_origem)),
            key="moeda_origem"
        )

        moeda_destino = st.selectbox(
            "Selecione a moeda de destino:",
            options=sorted(set(moedas_destino)),
            key="moeda_destino"
        )

        cambios = [linha[2] for linha in dados if linha[0] == moeda_origem and linha[1] == moeda_destino and linha[7] == tipo_valor]
        datas = [[linha[4], linha[5], linha[6]] for linha in dados]

        grafico_linha(
            f"💱 Variação Cambial - {moeda_origem} para {moeda_destino}",
            [f"{data[0]:02d}/{data[1]:02d}/{data[2]}" for data in datas],
            cambios,
            "Data",
            "Variação Cambial"
        )
    else:
        st.warning("Nenhum dado encontrado.")

# Distribuição por Meio de Transporte
elif aba == "Distribuição por Meio de Transporte":
    dados = consultaBanco.obter_percentual_transporte()
    if dados:
        meios = [linha[0] for linha in dados]
        percentuais = [linha[2] for linha in dados]

        fig = go.Figure(data=[go.Pie(
            labels=meios,
            values=percentuais,
            hole=0.4,
            textinfo='label+percent',
            textfont=dict(size=16),
            marker=dict(line=dict(color='#000000', width=2))
        )])

        fig.update_layout(
            title="🚢 Distribuição por Meio de Transporte",
            title_font=dict(size=24),
            legend_title="Modal",
            legend=dict(
                font=dict(size=18),
                orientation="v",
                x=1,
                y=0.5
            ),
            margin=dict(l=20, r=20, t=80, b=20)
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Nenhum dado encontrado.")
