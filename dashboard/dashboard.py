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
    "",
    [
        "Exportações por País",
        "Volume de Exportação por Produto",
        "Volume de Importação por Produto",
        "Valor Total Exportado por Ano",
        "Valor Total Importado por Ano",
        "Comércio por Bloco Econômico",
        "Parceiros Comerciais",
        "Variação Câmbio - Exportações",
        "Variação Câmbio - Importações",
        "Distribuição por Meio de Transporte"
    ]
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

# Exportações por País
if aba == "Exportações por País":
    dados = consultaBanco.obter_exportacoes_por_pais()
    if dados:
        paises = [linha[0] for linha in dados]
        valores = [linha[1] for linha in dados]
        grafico_colunas("🌍 Exportações por País", paises, valores, "País", "Total Exportado")
    else:
        st.warning("Nenhum dado encontrado.")

# Possibilidade, juntar importação e exportação em um gráfico de barras duplas

# Volume de Exportação por Produto
elif aba == "Volume de Exportação por Produto":
    dados = consultaBanco.obter_volume_exportacoes_por_produto()
    if dados:
        produtos = [linha[0] for linha in dados]
        volumes = [linha[1] for linha in dados]
        grafico_colunas("📦 Volume de Exportação por Produto", produtos, volumes, "Produto", "Volume Exportado")
    else:
        st.warning("Nenhum dado encontrado.")

# Volume de Importação por Produto
elif aba == "Volume de Importação por Produto":
    dados = consultaBanco.obter_volume_importacoes_por_produto()
    if dados:
        produtos = [linha[0] for linha in dados]
        volumes = [linha[1] for linha in dados]
        grafico_colunas("📥 Volume de Importação por Produto", produtos, volumes, "Produto", "Volume Importado")
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
        tipos_map = {"Imporações": "IMPORT", "Exportações": "EXPORT"}

        opcoes = list(tipos_map.keys())
        tipo_escolhido = st.radio("Selecione o tipo de transação", opcoes)
        
        tipo_valor = tipos_map.get(tipo_escolhido)

        st.success(f"Exibindo dados de {tipo_valor.lower()}ações.")

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
        parceiros_por_origem = {}

        for linha in dados:
            origem = linha[0]
            destino = linha[1]
            valor = float(linha[2])

            if origem not in parceiros_por_origem:
                parceiros_por_origem[origem] = []
            parceiros_por_origem[origem].append((destino, f"{valor:,.2f}"))

        for origem, destinos in parceiros_por_origem.items():
            # Título estilizado acima do expander
            st.markdown(
                f"<h4 style='margin-bottom:0.2em;'>📦 {origem} - Exportações para {len(destinos)} países</h4>",
                unsafe_allow_html=True
            )

            with st.expander("Clique para ver detalhes", expanded=False):
                for destino, valor in destinos:
                    st.markdown(
                        f"<p style='font-size:18px;'>➡️ <strong>{destino}</strong>: "
                        f"<span style='color:#1f77b4;'>${valor}</span></p>",
                        unsafe_allow_html=True
                    )
    else:
        st.warning("Nenhum dado encontrado.")


# Variação Câmbio - Exportações
elif aba == "Variação Câmbio - Exportações":
    dados = consultaBanco.obter_variacao_cambio_exportacoes()
    if dados:
        moeda_dados = {}
        for linha in dados:
            moeda_origem = linha[5]
            moeda_destino = linha[6]
            data_raw = linha[7]
            diferenca = float(linha[10])

            # Formatando a data como string legível
            if isinstance(data_raw, str):
                data_formatada = data_raw
            else:
                data_formatada = data_raw.strftime("%d/%m/%Y")

            par_moeda = f"{moeda_origem} - {moeda_destino}"

            if par_moeda not in moeda_dados:
                moeda_dados[par_moeda] = {"datas": [], "diferencas": []}

            moeda_dados[par_moeda]["datas"].append(data_formatada)
            moeda_dados[par_moeda]["diferencas"].append(diferenca)

        fig = go.Figure()
        for par, info in moeda_dados.items():
            fig.add_trace(go.Scatter(
                x=info["datas"],
                y=info["diferencas"],
                mode='lines+markers',
                name=par,
                line=dict(width=2),
                marker=dict(size=6)
            ))

        fig.update_layout(
            title=dict(
                text="💱 Variação Cambial - Exportações por Par de Moedas",
                font=dict(size=22)  # Tamanho do título
            ),
            xaxis_title="Data",
            yaxis_title="Diferença Cambial",
            template="plotly_white",
            height=550,
            font=dict(size=14),  # Tamanho geral da fonte
            legend=dict(
                title="Par de Moedas",
                font=dict(size=16),          # Tamanho da legenda
                title_font=dict(size=18)     # Tamanho do título da legenda
            ),
            xaxis_tickangle=-45,
            margin=dict(l=20, r=20, t=80, b=60)
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Nenhum dado encontrado.")

# Variação Câmbio - Importações
elif aba == "Variação Câmbio - Importações":
    dados = consultaBanco.obter_variacao_cambio_import()
    if dados:
        moeda_dados = {}
        for linha in dados:
            moeda_origem = linha[5]
            moeda_destino = linha[6]
            data_raw = linha[7]
            diferenca = float(linha[10])

            # Formatar a data como string legível
            if isinstance(data_raw, str):
                data_formatada = data_raw
            else:
                data_formatada = data_raw.strftime("%d/%m/%Y")

            par_moeda = f"{moeda_origem} - {moeda_destino}"

            if par_moeda not in moeda_dados:
                moeda_dados[par_moeda] = {"datas": [], "diferencas": []}

            moeda_dados[par_moeda]["datas"].append(data_formatada)
            moeda_dados[par_moeda]["diferencas"].append(diferenca)

        fig = go.Figure()
        for par, info in moeda_dados.items():
            fig.add_trace(go.Scatter(
                x=info["datas"],
                y=info["diferencas"],
                mode='lines+markers',
                name=par,
                line=dict(width=2),
                marker=dict(size=6)
            ))

        fig.update_layout(
            title=dict(
                text="💱 Variação Cambial - Importações por Par de Moedas",
                font=dict(size=22)  # Tamanho do título
            ),
            xaxis_title="Data",
            yaxis_title="Diferença Cambial",
            template="plotly_white",
            height=550,
            font=dict(size=14),  # Tamanho geral de fonte
            legend=dict(
                title="Par de Moedas",
                font=dict(size=16),  # Tamanho da legenda
                title_font=dict(size=18)  # Tamanho do título da legenda
            ),
            xaxis_tickangle=-45,
            margin=dict(l=20, r=20, t=80, b=60)
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Nenhum dado encontrado.")



## Distribuição por Meio de Transporte
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
                font=dict(size=18),  # Aumenta o tamanho da legenda
                orientation="v",     # Vertical (padrão)
                x=1,                 # Alinha à direita
                y=0.5
            ),
            margin=dict(l=20, r=20, t=80, b=20)
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Nenhum dado encontrado.")
