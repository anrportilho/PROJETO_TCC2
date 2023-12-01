import requests
from tqdm import tqdm
import funcoes.conn_bd
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
mycursor = mydb.cursor()
sql0 = "TRUNCATE TABLE tbluf"
mycursor.execute(sql0)
mycursor = mydb.cursor()
periodoselecionado = ['7', '15', '30', '45', '60', '90', '180']

for i in tqdm(range(len(periodoselecionado))):
    consulta = 'SELECT detalhereclamacao.reclamanteEstado AS uf,textostramites.clienteId AS empresa, SUM(CASE WHEN (textostramites.tramiteTipoCodigo = 6 AND tramiteTipoCodigo = 6 AND tramiteTextos = "S" AND detalhereclamacao.prazo BETWEEN date(date(now()) - INTERVAL {0} DAY) AND date(now()) ) THEN 1 ELSE 0 END) resolvida, SUM(CASE WHEN (textostramites.tramiteTipoCodigo = 6 AND tramiteTipoCodigo = 6 AND tramiteTextos = "N" AND detalhereclamacao.prazo BETWEEN date(date(now()) - INTERVAL {0} DAY) AND date(now())) THEN 1 ELSE 0 END) naoresolvida, SUM(CASE WHEN (situacaoCodigo = 6 AND textostramites.tramiteTipoCodigo = 1 AND detalhereclamacao.prazo BETWEEN date(date(now()) - INTERVAL {0} DAY) AND date(now()) ) THEN 1 ELSE 0 END) finalizadaAvaliada, SUM(CASE WHEN (situacaoCodigo = 7 AND textostramites.tramiteTipoCodigo = 1 AND detalhereclamacao.prazo BETWEEN date(date(now()) - INTERVAL {0} DAY) AND date(now()) ) THEN 1 ELSE 0 END) finalizadaNaoAvaliada from textostramites RIGHT JOIN detalhereclamacao ON textostramites.tramiteProtocoloReclamacao = detalhereclamacao.num_prot group by uf,empresa ORDER BY uf ASC'.format(
        periodoselecionado[i])
    mycursor.execute(consulta)
    myresult = mycursor.fetchall()
    qtd_itens = 5
    count = -1

    for a in range(len(myresult)):
        count += 1
        uf = myresult[count][0]
        empresa = myresult[count][1]
        resolvida = myresult[count][2]
        naoresolvida = myresult[count][3]
        finalizadaAvaliada = myresult[count][4]
        finalizadaNaoAvaliada = myresult[count][5]
        periodo = periodoselecionado[i]
        sql1 = "INSERT INTO tbluf(uf, clienteId, resolvida, naoResolvida, finalizadaAvaliada, finalizadaNaoAvaliada, periodo) values(%s,%s,%s,%s,%s,%s,%s)"
        val1 = (uf, empresa, resolvida, naoresolvida, finalizadaAvaliada, finalizadaNaoAvaliada, periodo)
        mycursor.execute(sql1, val1)

print("Sumarização completa.")
# Encerra conexão com o banco de dados, se houver.
if mydb.is_connected():
    mycursor.close()
    mydb.close()
    print("Conexão com o banco de dados foi encerrada")


