# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 15:53:01 2023

author: marcio.candido
Titulo: Programa para baixar do hidrotelemetria

Página com as especificações do hidrowebservice
https://www.ana.gov.br/hidrowebservice/swagger-ui.html#/
"""
import requests
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
import os
import time
import pyautogui


# Classe de importação de dados do hidrotelemetria
class get_hidrotelemetria:
    # Iniciar classe de dados 
    def __init__(self, arq, pasta, nmd, usuario, password):
        # Dados de Login
        self.usuario = usuario
        self.password = password
        
        # Arquivo com a lista das estações a serem baixadas e a data de inicio de cada upload 
        self.arq = arq
        
        # Pasta onde os arquivos txt estão ou vão ser gravados
        self.pasta = pasta
        
        # Número máximo de dias antes da data atual que poderá ser atualizado 
        # no dataset.txt como data final
        self.nmd = nmd
        
    # Obter autorização de acesso
    def get_auth(self):
        # Autenticação no servidor
        login = {'Identificador':self.usuario, 'Senha':self.password}
        contador = 0
        while contador<20:
            response = requests.get('https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/OAUth/v1', headers=login, timeout=120.0)
            status = response.status_code
            if status == 200:
                token = f"Bearer {response.json()['items']['tokenautenticacao']}"
                validade = response.json()['items']['validade']
                validade = dt.datetime.strptime(validade[:20] + validade[30:],'%a %b %d %H:%M:%S %Y')
                auth = {'Authorization': token}
                contador=20
            else:
                auth = " "
                validade = " "
                contador = contador+1
                time.sleep(10)
        return status, auth, validade

    # Baixar dados de cotas das estações telemétricas - máximo 1 dia
    def get_dados_tel(self, codigo, dtb, auth, tipo='DATA_LEITURA'):
        # Consulta aos dados
        consulta = {
            'Código da Estação':codigo,
            'Data de Busca (yyyy-MM-dd)':dtb,
            'Range Intervalo de busca':'HORA_24',
            'Tipo Filtro Data':tipo
            }
        contador = 0
        while contador<20:
            dados = requests.get('https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroinfoanaSerieTelemetricaAdotada/v1', headers=auth, params=consulta, timeout=120.0)
            
            # Avaliação da resposta
            if dados.status_code==200:
                dados = pd.DataFrame(dados.json()['items'])
                contador = 20
            else:
                contador = contador + 1
                if contador>= 20:
                    dados = pd.DataFrame()
                time.sleep(10)
        return dados

    # Baixar série de dados telemétricos
    def get_serie_tel(self, codigo, dti, dtf, tipo='DATA_LEITURA'):
        inicio = dt.datetime.strptime(dti,'%d/%m/%Y')
        fim = dt.datetime.strptime(dtf,'%d/%m/%Y')
        df = pd.DataFrame()
        status, auth, validade = self.get_auth()    
        flag = True
        if status==200:
            validade = dt.datetime.now() + dt.timedelta(minutes=10)
            while inicio<= fim and flag:
                # simula o aperto da tecla shift para não entrar
                # na tela de proteção do windows
                pyautogui.press('shift')
                data = dt.datetime.now()
                print(codigo, inicio, data, validade)
                if data<(validade - dt.timedelta(seconds=30)):
                    dtb = dt.datetime.strftime(inicio,'%Y-%m-%d')
                    aux = self.get_dados_tel(codigo, dtb, auth, tipo)
                    if not aux.empty:
                        df = pd.concat([df,aux])
                    inicio = inicio + dt.timedelta(days=1)
                else:
                    status, auth, validade = self.get_auth()
                    validade = dt.datetime.now() + dt.timedelta(minutes=10))
                    if status!= 200:
                        df=pd.DataFrame()
                        flag = False
                        print(f'Erro: Autenticação não autorizada = {codigo}')
        else:
            print(f'Erro: Autenticação não autorizada = {codigo}')
        return df.reset_index().drop_duplicates()

    # Baixar um batch de dados - versão 1
    def baixar_dados(self):
        agora = dt.datetime.now()
        dtf = agora.strftime('%d/%m/%Y')
        dtf_min = agora.date()-relativedelta(days=self.nmd)
        colunas = ['codigo', 'datahora','prec','cota','vazao' ] 
        df = pd.read_csv(self.arq, sep=',')
        for codigo, dti in df[['codigo','data']].values:
            arq_dados = f'{self.pasta}/{codigo}.txt'
            if os.path.exists(arq_dados):
                dados1 = pd.read_csv(arq_dados, sep=',')
                dados1['datahora'] = pd.to_datetime(dados1['datahora'])
                dados1 = dados1[dados1['datahora'] < dt.datetime.strptime(dti,'%d/%m/%Y')]
            else:
                dados1=pd.DataFrame(columns=colunas)
            dados2 = self.get_serie_tel(codigo,dti,dtf)
            if dados2.empty:
                dados = dados1
            else:
                dados2 = dados2[['codigoestacao','Data_Hora_Medicao','Chuva_Adotada',
                                 'Cota_Adotada','Vazao_Adotada']]
                dados2.columns = colunas
                dados = pd.concat([dados1, dados2], axis=0)
            dados.dropna(how='all',subset=['prec','cota','vazao'], inplace=True)
            if dados.empty:
                df.loc[df[df['codigo']==codigo].index[0],'data'] = dtf_min.strftime('%d/%m/%Y')
            else:
                dados.to_csv(arq_dados, sep=',', index=False)
                dtf_dados = str(pd.to_datetime(dados['datahora']).dropna().max())
                dtf_dados = dt.datetime.strptime(dtf_dados,'%Y-%m-%d %H:%M:%S')
                if dtf_min < dtf_dados.date():
                    df.loc[df[df['codigo']==codigo].index[0],'data'] = dtf_dados.strftime('%d/%m/%Y')
                else:
                    df.loc[df[df['codigo']==codigo].index[0],'data'] = dtf_min.strftime('%d/%m/%Y')
            df.to_csv(self.arq, sep=',', index=False)
        return 
    
    # Baixar um batch de dados - versão 2
    def baixar_dados2(self, ndias=5):
        agora = dt.datetime.now()
        colunas = ['codigo', 'datahora','prec','cota','vazao' ] 
        df = pd.read_csv(self.arq, sep=',')
        dados = []
        for codigo in df['codigo'].values:
            dtf = agora.strftime('%d/%m/%Y')
            dti = agora.date()-relativedelta(days=ndias)
            dti = dti.strftime('%d/%m/%Y')
            dados2 = self.get_serie_tel(codigo,dti,dtf)
            if not dados2.empty:
                dados2 = dados2[['codigoestacao','Data_Hora_Medicao','Chuva_Adotada',
                                 'Cota_Adotada','Vazao_Adotada']]
                dados2.columns = colunas
                dados2['datahora'] = pd.to_datetime(dados2['datahora'])
                dados2.set_index('datahora', inplace=True)
                tipos = {'prec':float, 'cota':float, 'vazao':float}
                dados2 = dados2.astype(tipos) 
                dados2 = dados2.resample('D').mean()
                dados2['codigo'] = codigo
                dados.append(dados2)
        if len(dados)>1:
            dados = pd.concat(dados, axis=0)
        else:
            dados = pd.DataFrame(columns=colunas)
        return dados
    