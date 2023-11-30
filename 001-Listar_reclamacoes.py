import funcoes.conn_api
import funcoes.conn_bd
import json
from datetime import datetime
from tqdm import tqdm

# Conecta com banco de dados
# ----------------------------
mydb = funcoes.conn_bd.connect()
mycursor = mydb.cursor()
# ----------------------------

mycursor.execute("SELECT cliente_id, cod, chave, cpf FROM cod_empresa")
myresult = mycursor.fetchall()
print(len(myresult))

for i in range(len(myresult)):
    cliente_id, cod, chave, cpf = myresult[i]
    print("Iniciando conexão com a API")
    request = funcoes.conn_api.autenticacao(cliente_id,cod,chave,cpf)
    todo = json.loads(request.content)
    listOfValues = todo.values()
    listOfValues = list(listOfValues)
    reclamacao = listOfValues[0]

    for i in tqdm(range(len(reclamacao))):

        dt_Prazo = datetime.strptime(reclamacao[i]['prazo'], "%d/%m/%Y %H:%M:%S")
        dtPrazo = dt_Prazo.strftime('%Y-%m-%d %H:%M:%S')
        dt_Abertura= datetime.strptime(reclamacao[i]['dataAbertura'], "%d/%m/%Y %H:%M:%S")
        dtAbertura = dt_Abertura.strftime('%Y-%m-%d %H:%M:%S')
        protocolo = reclamacao[i]['protocolo']
        situacaoCodigo = reclamacao[i]['situacao']['codigo']
        situacaoDescricao = reclamacao[i]['situacao']['descricao']
        prazo = dt_Prazo
        dataAbertura = dtAbertura
        clienteId = cliente_id
        sql="INSERT IGNORE INTO reclamacoes (protocolo,situacaoCodigo,situacaoDescricao,prazo,dataAbertura,clienteId) values(%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE situacaoCodigo =%s ,situacaoDescricao =%s ,prazo = %s"
        val= (protocolo,situacaoCodigo,situacaoDescricao,prazo,dataAbertura,clienteId,situacaoCodigo,situacaoDescricao,prazo)
        mycursor.execute(sql, val)
    mydb.commit()

print("Dados gravados no banco de dados")
# Encerra conexão com o banco de dados, se houver.
if mydb.is_connected():
    mycursor.close()
    mydb.close()
    print("Conexão com o banco de dados foi encerrada")