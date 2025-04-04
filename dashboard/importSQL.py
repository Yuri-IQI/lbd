import consultaBanco

exportacoes = consultaBanco.obter_exportacoes_por_pais()
print("Exportações por país:")
for pais, total in exportacoes:
    print(f"{pais}: R$ {total:,.2f}")

volume_exportacoes = consultaBanco.obter_volume_exportacoes_por_produto()
print("\nVolume de exportações por produto:")
for produto, volume in volume_exportacoes:  
    print(f"{produto}: {volume}")  

volume_importacoes = consultaBanco.obter_volume_importacoes_por_produto()
print("\nVolume de importações por produto:")
for produto, volume in volume_importacoes:  
    print(f"{produto}: {volume}")  

    
evolucao_comercio = consultaBanco.obter_evolucao_comercio_por_bloco()
print("\nEvolução do comercio por bloco:")
for bloco_economico, total_comercializado in evolucao_comercio:
    print(f"{bloco_economico}: R$ {total_comercializado:,.2f}")    
    
parceiros_comerciais = consultaBanco.obter_parceiros_comerciais()
print("\nPrincipais parceiros comerciais de cada país:")
for pais, parceiro, total in parceiros_comerciais:
    print(f"{pais} -> {parceiro}: {total}")
    

variacao_exportacoes = consultaBanco.obter_variacao_cambio_exportacoes()
print("\nVariação do câmbio:")
for linha in variacao_exportacoes:
    transacao_id, pais_origem, pais_destino, produto, tipo_transacao, moeda_origem_nome, moeda_destino_nome, data_cambio, taxa_cambio, taxa_cambio_anterior, diferenca_variacao, valor_transacao, quantidade, diferenca_valor = linha
    print(f"{pais_origem} -> {pais_destino}: {diferenca_variacao} (Diferença: {diferenca_valor})")
  

variacao_importacoes = consultaBanco.obter_variacao_cambio_import()
print("\nVariação do câmbio para IMPORTAÇÕES:")
for linha in variacao_importacoes:
    transacao_id, pais_origem, pais_destino, produto, tipo_transacao, moeda_origem_nome, moeda_destino_nome, data_cambio, taxa_cambio, taxa_cambio_anterior, diferenca_variacao, valor_transacao, quantidade, diferenca_valor = linha
    print(f"{pais_origem} -> {pais_destino}: {diferenca_variacao} (Diferença: {diferenca_valor})")
   

transporte_utilizado = consultaBanco.obter_percentual_transporte()
print("\nTransporte utilizado:")
for meio_transporte, total_transacoes, percentual in transporte_utilizado:
    print(f"{meio_transporte}: {total_transacoes} transações ({percentual}%)")

exportacao_por_ano = consultaBanco.obter_total_exportado_por_ano()
print("\nExportações por ano:")
for ano, total in exportacao_por_ano:
    print(f"{ano}: R$ {total:,.2f}")

importacao_por_ano = consultaBanco.obter_total_importado_por_ano()
print("\nImportações por ano:")
for ano, total in importacao_por_ano:
    print(f"{ano}: R$ {total:,.2f}")