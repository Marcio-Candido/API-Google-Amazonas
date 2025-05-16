# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 14:42:07 2025
Programa para importar as previsões da API-Google e 
compatibilizar com os dados do Hidrotelemetria
@author: marcio.candido
"""

# Importando as bibliotecas
import requests
import datetime as dt
import pandas as pd
from matplotlib import pyplot as plt
import matplotlib.image as mpimg
from bs4 import BeautifulSoup
import numpy as np
import hidrotelemetria as dat

# CONSTANTES
# Token de autenticação google
KEY = 'AIzaSyDmFWQWhI_BAly6eXFDWKCXpmyn6kH3q4E'
region_code = "BR"

# Arquivo de configuração dos dados
arq_conf = 'inputs/config.csv'

# Arquivo com as curvas-chaves
arq_curvas = 'inputs/curvas.csv'
curvas = pd.read_csv(arq_curvas)
curvas['inicio'] = pd.to_datetime(curvas['inicio'])
curvas['fim'] = pd.to_datetime(curvas['fim'])

# Carregar a imagem de marca d'água
logo = 'inputs/simbolo2.png'
imagem = mpimg.imread(logo)

# Diretorio onde as imagens vão ser gravadas
result = 'imagens'

# DEFINIÇÕES DE FUNÇÕES
# API-Google
# Limites de alertas
def limites(codigo):
    res =  requests.get('https://floodforecasting.googleapis.com/v1/gaugeModels:batchGet?' \
                        f'key={KEY}&names=gaugeModels/{codigo}').json()
    return res

# Baixar previsão
def previsao(codigo, start, end):
    res = requests.get(
        'https://floodforecasting.googleapis.com/v1/gauges:queryGaugeForecasts',
        params={
            'key':KEY,
            'gaugeIds': codigo,
            'issuedTimeStart': start,
            'issuedTimeEnd': end,
        },
    ).json()
    prevs = res['forecasts'] if res and 'forecasts' in res else {}
    if prevs:
        prevs = prevs[codigo]['forecasts'] if prevs and 'forecasts' in prevs[codigo] else {}
        prevs = max(prevs, key=lambda prev: prev['issuedTime']) if prevs else {}
        df = pd.DataFrame(prevs['forecastRanges'])
        df['issuedTime'] = prevs['issuedTime']
        df['gaugeId'] = prevs['gaugeId']
        df.drop('forecastEndTime',axis=1,inplace=True)
        df.columns = ['valor','data', 'data_prev','codigo']
        df['data'] = pd.to_datetime(df['data'])
        df['data_prev'] = pd.to_datetime(df['data_prev'])
        df.set_index('data', inplace=True)
        df = df[['data_prev', 'codigo','valor']]
        res = limites(codigo)
        niveis = res['gaugeModels'] if res and 'gaugeModels' in res else {}
        if niveis:
            limiares = niveis[0]['thresholds'].keys()
            converte = {'warningLevel': 'atencao',
                        'dangerLevel': 'alerta',
                        'extremeDangerLevel': 'inundacao'}
            for limiar in limiares:
                df[converte[limiar]] = niveis[0]['thresholds'][limiar]
            df['unidade'] = niveis[0]['gaugeValueUnit']
            df['qualityVerified'] = niveis[0]['qualityVerified']
    else:
        df = pd.DataFrame()
    return df

# Baixar previsões dos pontos cadastrados em config
def previsoes(config):
    inicio = (dt.datetime.now() - dt.timedelta(days=7)).strftime('%Y-%m-%d')
    fim = (dt.datetime.now() + dt.timedelta(days=1)).strftime('%Y-%m-%d')
    previsoes = []
    print('Inicio do processo de upload das previsoes:')
    for codigo, titulo in config[['id_google','nome']].values:
        print(f'  > upload dos dados de previsão: {codigo}')
        prev = previsao(codigo, inicio, fim)
        if not prev.empty:
            previsoes.append(prev)
        else:
            print(f'Erro no upload dos dados do ponto: {codigo}')
    if previsoes:
        # Juntar as previsões em um dataframe 
        df = pd.concat(previsoes, axis=0)
        print('Fim do processo de upload das previsões')
        return df
    else:
        print('Fim do processo de upload das previsões')
        return pd.DataFrame()

# CURVAS-CHAVE
# Calculo da vazao
def cal_vazao(codigo, data, h):
    df_cc = curvas[(curvas['codigo']==codigo) 
                   & (curvas['inicio']<= data) & (curvas['fim']>= data)
                   & (curvas['cmin']<= h) & (curvas['cmax']>= h)]
    if  not df_cc.empty:
        a = df_cc['a'].iloc[0]
        h0 = df_cc['h0'].iloc[0]
        n = df_cc['n'].iloc[0]
        if (h/100)>h0:
            return a*(h/100-h0)**n
        else:
            return 0
    else:
        return np.nan

# Calculo da cota
def cal_cota(codigo, data, q):
    df_cc = curvas[(curvas['codigo']==codigo) 
                   & (curvas['inicio']<= data) & (curvas['fim']>= data)
                   & (curvas['qmax']>= q)]
    if  not df_cc.empty:
        a = df_cc['a'].iloc[0]
        h0 = df_cc['h0'].iloc[0]
        n = df_cc['n'].iloc[0]
        return (h0+(q/a)**(1/n))*100
    else:
        return np.nan

# Verifica se tem curva-chave
def tem_curva(codigo, data):
    df_cc = curvas[(curvas['codigo']==codigo) 
                   & (curvas['inicio']<= data) & (curvas['fim']>= data)]
    if  not df_cc.empty :
        return True
    else:
        return False   
    
# limiares de vazao
def amplitude(codigo, data):
    df_cc = curvas[(curvas['codigo']==codigo) 
                   & (curvas['inicio']<= data) & (curvas['fim']>= data)]
    if  not df_cc.empty :
        return df_cc['cmin'].min(), df_cc['cmax'].max(), df_cc['qmin'].min(), df_cc['qmax'].max()
    else:
        return np.nan

    
# Baixar os dados do hidrotelemetria
def monitor():
    # Dados de Login
    usuario = '00118378619'
    password = 'fuzzyclips'
    # Arquivo com a lista das estações a serem baixadas e a data de inicio de cada upload 
    arq = r'inputs/config.csv'
    # Pasta onde os arquivos txt estão ou vão ser gravados
    pasta = r'dados'
    # Número máximo de dias antes da data atual que poderá ser atualizado 
    # no dataset.txt como data final
    nmd = 7
    base = dat.get_hidrotelemetria(arq, pasta, nmd, usuario, password)
    dados =  base.baixar_dados2(nmd)
        
    return dados

# GRAFICOS
# Gerar gráficos

def graficos(config, google, monit):  
    # Correção do tipo do valor
    google.valor = google.valor.astype(float)
    # Gerar graficos
    codigos = google['codigo'].unique()
    for codigo in codigos:
        cod = config[config['id_google']== codigo]['codigo'].iloc[0]
        nome = config[config['id_google']== codigo]['nome'].iloc[0]
        print(codigo, cod, nome)
        df1 = google[google['codigo']== codigo].copy().reset_index()
        df1['data'] = df1['data'].dt.tz_localize(None)
        df1.columns = ['datahora', 'data_prev', 'codigo', 'valor', 'atencao', 'alerta', 'inundacao',
           'unidade', 'qualityVerified']
        df1.set_index('datahora', inplace=True)
        data = dt.datetime(df1.index[0].year, df1.index[0].month, df1.index[0].day)
        ampl = amplitude(cod, data)
        limiares = config[config['id_google']== codigo]['cota_ref'].iloc[0]

        if tem_curva(cod, data):
            # Limiares
            if df1['unidade'][0]=='METERS':
                df1['valor'] = [cal_vazao(cod, data, valor*100) for valor in df1['valor'].values]
            
            # Assimilação de erro
            df4 = df1.resample('D').mean()
            df2 = monit[monit['codigo']==cod]['vazao']
            df3 = pd.concat([df2,df4], axis=1)
            df3['erro'] = df3['vazao'] - df3['valor']
            data = df3['vazao'].last_valid_index()
            erro = df3.loc[data,'erro']
            df3['valor_2'] = df3['valor'] + erro 
            
            # Gráfico
            fig, ax = plt.subplots(figsize=(10,6), dpi=300)
            df3['vazao'].plot(ax=ax,label='Observado', linewidth = 3)
            df3['valor_2'][df3.index>=data].plot(ax=ax,label='Previsão ajustada', color='black', linewidth = 2, linestyle='--')
            df3['valor'].plot(ax=ax,label='API-Google', color='red', linewidth = 1, linestyle=':')
            
            if limiares == 'S':
                atencao = config[config['id_google']== codigo]['atencao'].iloc[0]
                alerta = config[config['id_google']== codigo]['alerta'].iloc[0]
                inundacao = config[config['id_google']== codigo]['inundacao'].iloc[0]
                atencao = cal_vazao(cod, data, atencao)
                alerta  = cal_vazao(cod, data, alerta)
                inundacao = cal_vazao(cod, data, inundacao)
                plt.axhline(y=atencao, color='yellow', linestyle='--',linewidth = 2, label='atenção')
                plt.axhline(y=alerta, color='orange', linestyle='--',linewidth = 2, label='alerta')
                plt.axhline(y=inundacao, color='red', linestyle='--',linewidth = 2, label='inundação')
            
            plt.axvline(x=data,color='black', linestyle='--',linewidth = 1)

            # configuração dos eixos
            # Eixo principal
            vz_lim = ax.get_ylim()
            if vz_lim[0]<0:
                ax.set_ylim(0,vz_lim[1])
                vz_lim = ax.get_ylim()
            h_lim = [cal_cota(cod, data,vz_lim[0]),cal_cota(cod, data,vz_lim[1])]
            
            if np.isnan(h_lim[0]) or h_lim[0]<ampl[0]:
                h_lim[0]= ampl[0]
            if np.isnan(h_lim[1]) or h_lim[0]>ampl[1]:
                h_lim[1]= ampl[1]
            
            h_lim[0] = int(h_lim[0]/10)*10
            h_lim[1] = int(h_lim[1]/10)*10 + 10
            
            if np.isnan(h_lim[0]) or h_lim[0]<ampl[0]:
                h_lim[0]= ampl[0]
            if np.isnan(h_lim[1]) or h_lim[0]>ampl[1]:
                h_lim[1]= ampl[1]
                
            vz_lim = [cal_vazao(cod, data,h_lim[0]),cal_vazao(cod, data,h_lim[1])]
            if np.isnan(vz_lim[0]):
                vz_lim[0]= ampl[2]
            if np.isnan(vz_lim[1]):
                vz_lim[1]= ampl[3]
                
            ax.set_ylim(vz_lim)
            size = int(np.round((h_lim[1]-h_lim[0])/80,0)*10)
            if (h_lim[0] % size)>0:
                base = h_lim[0] + (size - (h_lim[0] % size))
            else:
                base = h_lim[0]
            labels = np.arange(base, h_lim[1], size)
            ticks = [cal_vazao(cod, data, q) for q in labels]
            ax.set_yticks(ticks)
            ax.set_yticklabels(labels)

            # Adicionar a imagem como fundo
            x_lim = ax.get_xlim()
            y_lim = ax.get_ylim() 
            limite = [x_lim[0],x_lim[1], y_lim[0],y_lim[1]]
            ax.imshow(imagem, extent= limite, alpha=0.2, aspect='auto', zorder=0)

            # Eixo secundario
            ax1 = ax.twinx()
            ax1.set_ylim(vz_lim)
            ax1.set_yticks(ticks)

            ax.grid(True)
            ax.legend()
            ax.set_ylabel('cota (cm)')
            ax1.set_ylabel('vazao (m3/s)')
            ax.set_title(f'{cod} - {nome}', fontsize=14, fontweight="bold")

            plt.tight_layout()
            salvarin =  f'{result}/{codigo}.png'
            plt.savefig(salvarin)
            # plt.show()   

        else:
            # Gráfico
            fig, ax = plt.subplots(figsize=(10,6), dpi=300)
            df1.valor.plot(ax=ax,label='API-Google', linewidth = 3)
             # Limiares
            if limiares=='S':
                atencao = config[config['id_google']== codigo]['atencao'].iloc[0]
                alerta = config[config['id_google']== codigo]['alerta'].iloc[0]
                inundacao = config[config['id_google']== codigo]['inundacao'].iloc[0]
                plt.axhline(y=atencao, color='yellow', linestyle='--',linewidth = 2, label='atenção')
                plt.axhline(y=alerta, color='orange', linestyle='--',linewidth = 2, label='alerta')
                plt.axhline(y=inundacao, color='red', linestyle='--',linewidth = 2, label='inundação')
           
            # Adicionar a imagem como fundo
            x_lim = ax.get_xlim()
            y_lim = ax.get_ylim() 
            limite = [x_lim[0],x_lim[1], y_lim[0],y_lim[1]]
            ax.imshow(imagem, extent= limite, alpha=0.2, aspect='auto', zorder=0)

            ax.grid(True)
            ax.legend()
            ax.set_ylabel('vazao (m3/s)')
            ax.set_title(f'{cod} - {nome}', fontsize=14, fontweight="bold")

            plt.tight_layout()
            salvarin =  f'{result}/{codigo}.png'
            plt.savefig(salvarin)
            # plt.show()


# ROTINA PRINCIPAL
if __name__=='__main__':
    config = pd.read_csv(arq_conf)
    config = config[config['ativar']=='S']
    # google = previsoes(config)
    monit = monitor()
    # graficos(config, google, monit)
    


#Fim