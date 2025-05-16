# -*- coding: utf-8 -*-
"""
Created on Mon May 12 14:18:09 2025

@author: marcio.candido
Programa para baixar dados do SACE
"""
# Importando as bibliotecas
import requests
import datetime as dt
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np

# Classe para baixar dados do SACE
class get_sace: 
    # inicializar configuração
    def __init__(self, config):
        config = pd.read_csv(arq_conf)
        return

    # Função para coleta dos dados no SACE
    def get_sace(self, codigo, id_sace, sace, horas=120):
        url = f'https://sace.sgb.gov.br/{sace}/rest/report?ID={id_sace}&HR={horas}'
        # Fazendo a requisição
        response = requests.get(url)
    
        # Verificando se a requisição foi bem-sucedida
        if response.status_code == 200:
            # Parsing do HTML usando BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', id='container')
            rows = table.find_all('tr')
            data = []
            for row in rows[1:]:
                cols = row.find_all('td')
                aux =[codigo]
                linha = [col.text.strip() if col.text.strip() != '' else None for col in cols]
                datahora = dt.datetime.strptime(f'{linha[0]} {linha[1]}','%d/%m/%Y %H:%M')
                aux.append(datahora)
                if linha[2] is None:
                    chuva = np.nan
                else:
                    chuva = float(linha[2].replace(',','.'))
                aux.append(chuva)
                if len(linha)>3:
                    if linha[3] is None:
                        cota = np.nan
                    else:
                        cota = float(linha[3].replace(',','.'))
                    aux.append(cota)
                
                data.append(aux)
            cols = [col.text.strip() for col in rows[0].find_all('td')]
            if len(cols)>3:
                if 'COTA' in cols[2].upper():
                    colunas = ['codigo','datahora','cota' ,'chuva']
                else:
                    colunas = ['codigo','datahora','chuva','cota']
            else:
                if 'COTA' in cols[2].upper():
                    colunas = ['codigo','datahora','cota']
                else:
                    colunas = ['codigo','datahora','chuva']
            df = pd.DataFrame(data, columns= colunas)
        else:
            df = pd.DataFrame()
        return df
    
    # Baixar os dados de monitoramento do SACE
    def baixar_dados(self):
        dados = []
        print('Inicio do processo de upload do sace:')
        for codigo, id_sace, sace in self.config[['codigo_ana','id_sace','sace']].values:
            print(f'  > upload os dados da estação: {codigo}')
            aux = get_sace(codigo, id_sace,sace)
            if not aux.empty:
                dados.append(aux)
        if dados:
            dados = pd.concat(dados, axis=0)
            print('calculando os dados de vazão')
            dados['vazao'] = [np.nan if np.isnan(cota) else cal_vazao(codigo, data, cota)
                              for codigo, data, cota in dados[['codigo', 'datahora', 'cota']].values]
            dados['datahora'] = pd.to_datetime(dados['datahora'])
            dados.set_index('datahora', inplace=True)
            print('Fim do processo de upload')
            return dados
        else:
            print('Nenhum dado foi carregado')
            print('Fim do processo de upload')
        return pd.DataFrame()