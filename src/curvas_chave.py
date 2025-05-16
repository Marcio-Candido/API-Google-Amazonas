# -*- coding: utf-8 -*-
"""
Created on Mon May 12 12:27:05 2025

@author: marcio.candido
Baixar os dados das curvas chave no SQL da RHN
"""

import pyodbc
import pandas as pd
from datetime import datetime
import pickle

# Configurações de conexão
server = '10.200.0.105'
username = ''
password = ''

# Arquivo com a descrição das estações para fazer a atualização das curvas-chaves
lista = r'C:\Agenda\APIs Google _hidrotelemetria\input\00_dataset.csv'

# Arquivo com as curvas atualizadas
arq = r'C:\Agenda\APIs Google _hidrotelemetria\input\curvas.plk'

df = pd.read_csv(lista, sep=',')
df = df[['codigo','banco']]
bancos = df['banco'].unique()
curvas = []

# Criação da conexão
try:
    for banco in bancos:
        # Estações do plano de trabalho
        estacoes = df[df['banco']==banco]['codigo'].values
        
        # Consulta a base SQL no servidor
        database = banco
        str_con =  f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        con = pyodbc.connect(str_con)
        sql = "select * from CurvaDescarga where Importado=0 and Temporario=0 and Removido=0"
        aux = pd.read_sql(sql, con)
        con.close()
        flag = [True if codigo in estacoes else False for codigo in aux['EstacaoCodigo']]
        curvas.append(aux[flag])
        print(f"Conexão bem-sucedida!: {banco}")
    curvas = pd.concat(curvas, axis=0, ignore_index=True)
    colunas =  [ 'EstacaoCodigo', 'PeriodoValidadeInicio', 'PeriodoValidadeFim', 'CotaMaxima',
                 'CotaMinima', 'CoefA','CoefH0', 'CoefN']
    curvas = curvas[colunas]
    curvas.columns = ['codigo','inicio','fim','cmax','cmin','a','h0', 'n']
    curvas['qmin'] = curvas['a']*(curvas['cmin']/100-curvas['h0'])**curvas['n']
    curvas['qmax'] = curvas['a']*(curvas['cmax']/100-curvas['h0'])**curvas['n']
    curvas.sort_values(['codigo','inicio','cmin'], inplace=True)
    curvas.to_pickle(arq)
except pyodbc.Error as err:
    print(f"Erro ao conectar: {err}")