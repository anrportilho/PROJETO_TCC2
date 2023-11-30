import requests
from datetime import datetime
from datetime import date, timedelta

# Data de incio e fim da consulta de dados que serão consumidos da API
dtinicio = '01-01-2023'
dtfim ='28-11-2023'
# ---------------------------------------------------------------------------

dtinicio2 = dtinicio
data_atual = date.today()
data_fim_lista = data_atual.strftime('%d-%m-%Y')
data_selecionada = data_atual - timedelta(5)
data_inicio_lista = data_selecionada.strftime('%d-%m-%Y')

def datas():
    dt_inicio = datetime.strptime(dtinicio, "%d-%m-%Y")
    datinicio = dt_inicio.strftime('%Y-%m-%d')
    dt_fim = datetime.strptime(dtfim, "%d-%m-%Y")
    datafim = dt_fim.strftime('%Y-%m-%d')
    dt_inicio2 = datetime.strptime(dtinicio2, "%d-%m-%Y")
    datinicio2 = dt_inicio2.strftime('%Y-%m-%d')
    return datinicio,datafim, datinicio2

def autenticacao(id,cod,chave,cpf):
    URL: '{url-ambiente}/seguranca/empresa/token'
    CLIENT_ID = id
    CODE = cod
    CLIENT_SECRET = chave
    GRANT_TYPE = "client_credentials"
    params = {
        "client_id": CLIENT_ID,
        "code": CODE,
        "client_secret": CLIENT_SECRET,
        "grant_type": GRANT_TYPE,
    }
    headers = {"Accept": "application/json",
               "User-Agent": "api-consumidor",
               "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
               }
    endpoint = "https://api.consumidor.gov.br/api/servico/seguranca/empresa/token"
    response = requests.post(endpoint, params=params, headers=headers).json()
    access_token = response['access_token']
    print("API conectada e carregando os dados...")
    chave = "Bearer " + str(access_token)

    # listar reclamações
    url_listar = "https://api.consumidor.gov.br/api/servico/reclamacoes/listar"
    params = {
        "dataIniPeriodo": data_inicio_lista,
        "dataFimPeriodo": data_fim_lista,
    }

    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "Host": "api.consumidor.gov.br",
        "Accept": "application/json",
        "User-Agent": "api-consumidor",
        "Authorization": chave,
        "versao": "1.0",
        "cpfAutorizado": cpf
    }

    request = requests.get(url_listar, headers=headers, params=params)
    return request

def autenticacao_detalhe(id,cod,chave,cpf):
    URL: '{url-ambiente}/seguranca/empresa/token'
    CLIENT_ID = id
    CODE = cod
    CLIENT_SECRET = chave
    GRANT_TYPE = "client_credentials"
    params = {
        "client_id": CLIENT_ID,
        "code": CODE,
        "client_secret": CLIENT_SECRET,
        "grant_type": GRANT_TYPE,
    }
    headers = {"Accept": "application/json",
               "User-Agent": "api-consumidor",
               "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
               }
    endpoint = "https://api.consumidor.gov.br/api/servico/seguranca/empresa/token"
    response = requests.post(endpoint, params=params, headers=headers).json()
    print(response)
    access_token = response['access_token']
    print("API Conectada")
    chave = "Bearer " + str(access_token)
    params = {
        "dataIniPeriodo": data_inicio_lista,
        "dataFimPeriodo": data_fim_lista,
    }
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "Host": "api.consumidor.gov.br",
        "Accept": "application/json",
        "User-Agent": "api-consumidor",
        "Authorization": chave,
        "versao": "1.0",
        "cpfAutorizado": cpf
    }
    return headers,params
