import requests
import funcoes.conn_bd
from tqdm import tqdm
import socket
from urllib3.connection import HTTPConnection
headers = requests.utils.default_headers()

HTTPConnection.default_socket_options = (
        HTTPConnection.default_socket_options + [
    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    (socket.SOL_TCP, socket.TCP_KEEPIDLE, 45),
    (socket.SOL_TCP, socket.TCP_KEEPINTVL, 10),
    (socket.SOL_TCP, socket.TCP_KEEPCNT, 6)
]
)

# Conecta com banco de dados
# ----------------------------
mydb = funcoes.conn_bd.connect()
mycursor = mydb.cursor()
# ----------------------------

mycursor.execute("SELECT cliente_id, cod, chave, cpf FROM cod_empresa")
myresult = mycursor.fetchall()
cliente_id = myresult[0][0]

mycursor = mydb.cursor()
sql0 = "TRUNCATE TABLE tblindicedesolucao"
mycursor.execute(sql0)

mycursor = mydb.cursor()

sinal = ['=', '=', '=', 'LIKE']
vetor = [cliente_id]
periodoselecionado = ['7', '30']
year = ['2023']

for z in range(len(year)):
    print(year[z])

    for i in tqdm(range(len(vetor))):
        for a in range(len(periodoselecionado)):

            consulta = '''SELECT
            SUM(CASE WHEN (textostramites.tramiteTipoCodigo = 6 AND tramiteTextos = 'S' AND YEAR(textostramites.tramiteData) = {2} AND reclamacoes.prazo BETWEEN date((date(now()) - INTERVAL {1} DAY)) AND date((date(now()) - INTERVAL 1 DAY))) THEN 1 ELSE 0 END) resolvida,
            SUM(CASE WHEN (textostramites.tramiteTipoCodigo = 6 AND tramiteTextos = 'N' AND YEAR(textostramites.tramiteData) = {2} AND reclamacoes.prazo BETWEEN date((date(now()) - INTERVAL {1} DAY)) AND date((date(now()) - INTERVAL 1 DAY)))  THEN 1 ELSE 0 END) naoresolvida,

            -- FINALIZADAS AVALIADAS E NÃO AVALIADAS
            SUM(CASE WHEN (reclamacoes.situacaoCodigo = 6 AND textostramites.tramiteTipoCodigo = 1 AND YEAR(textostramites.tramiteData) = {2} AND reclamacoes.prazo BETWEEN date((date(now()) - INTERVAL {1} DAY)) AND date((date(now()) - INTERVAL 1 DAY))) THEN 1 ELSE 0 END) finalizadaAvaliada,
            SUM(CASE WHEN (reclamacoes.situacaoCodigo = 7 AND textostramites.tramiteTipoCodigo = 1 AND YEAR(textostramites.tramiteData) = {2} AND reclamacoes.prazo BETWEEN date((date(now()) - INTERVAL {1} DAY)) AND date((date(now()) - INTERVAL 1 DAY))) THEN 1 ELSE 0 END) finalizadaNaoAvaliada,

            SUM(CASE WHEN (textostramites.tramiteTipoCodigo = 7 AND tramiteTextos = '1' AND YEAR(textostramites.tramiteData) = {2} AND reclamacoes.prazo BETWEEN date((date(now()) - INTERVAL {1} DAY)) AND date((date(now()) - INTERVAL 1 DAY))) THEN 1 ELSE 0 END) nota1,
            SUM(CASE WHEN (textostramites.tramiteTipoCodigo = 7 AND tramiteTextos = '2' AND YEAR(textostramites.tramiteData) = {2} AND reclamacoes.prazo BETWEEN date((date(now()) - INTERVAL {1} DAY)) AND date((date(now()) - INTERVAL 1 DAY))) THEN 1 ELSE 0 END) nota2,
            SUM(CASE WHEN (textostramites.tramiteTipoCodigo = 7 AND tramiteTextos = '3' AND YEAR(textostramites.tramiteData) = {2} AND reclamacoes.prazo BETWEEN date((date(now()) - INTERVAL {1} DAY)) AND date((date(now()) - INTERVAL 1 DAY))) THEN 1 ELSE 0 END) nota3,
            SUM(CASE WHEN (textostramites.tramiteTipoCodigo = 7 AND tramiteTextos = '4' AND YEAR(textostramites.tramiteData) = {2} AND reclamacoes.prazo BETWEEN date((date(now()) - INTERVAL {1} DAY)) AND date((date(now()) - INTERVAL 1 DAY))) THEN 1 ELSE 0 END) nota4,
            SUM(CASE WHEN (textostramites.tramiteTipoCodigo = 7 AND tramiteTextos = '5' AND YEAR(textostramites.tramiteData) = {2} AND reclamacoes.prazo BETWEEN date((date(now()) - INTERVAL {1} DAY)) AND date((date(now()) - INTERVAL 1 DAY))) THEN 1 ELSE 0 END) nota5,
            reclamacoes.clienteId AS clienteId
            from textostramites
            RIGHT JOIN reclamacoes
            ON
            textostramites.tramiteProtocoloReclamacao = reclamacoes.protocolo
            WHERE reclamacoes.clienteId {3} {0}'''.format(vetor[i], periodoselecionado[a], year[z], sinal[i])

            # print(consulta)
            mycursor.execute(consulta)
            myresult = mycursor.fetchall()
            print("Índicadores calculados")
            qtd_itens = 5
            count = -1
            # Grava no banco de dados
            for _ in range(len(myresult)):
                count += 1
                resolvida = myresult[count][0]
                naoresolvida = myresult[count][1]
                finalizadaAvaliada = myresult[count][2]
                finalizadaNaoAvaliada = myresult[count][3]
                nota1 = myresult[count][4]
                nota2 = myresult[count][5]
                nota3 = myresult[count][6]
                nota4 = myresult[count][7]
                nota5 = myresult[count][8]
                periodo = periodoselecionado[a]
                clienteId = cliente_id
                ano = year[z]

                sql1 = "INSERT INTO  tblindicedesolucao(resolvida,naoresolvida,finalizadaAvaliada,finalizadaNaoAvaliada,nota1,nota2,nota3,nota4,nota5,periodo,clienteId,ano) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                val1 = (
                resolvida, naoresolvida, finalizadaAvaliada, finalizadaNaoAvaliada, nota1, nota2, nota3, nota4, nota5,
                periodo, clienteId, ano)
                mycursor.execute(sql1, val1)
                mydb.commit()

# Encerra conexão com o banco de dados, se houver.
if mydb.is_connected():
    mycursor.close()
    mydb.close()
    print("Conexão com o banco de dados foi encerrada.")


