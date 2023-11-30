import funcoes.conn_api
import funcoes.conn_bd

# Conecta com banco de dados
# ----------------------------
mydb = funcoes.conn_bd.connect()
mycursor = mydb.cursor()
sql0 = "TRUNCATE TABLE tblentradasdiaria"
mycursor.execute(sql0)
mycursor = mydb.cursor()

mycursor.execute("SELECT cliente_id, cod, chave, cpf FROM cod_empresa")
myresult = mycursor.fetchall()
cliente_id = myresult[0][0]
# ----------------------------

mydata = funcoes.conn_api.datas()
sinal = ['=']
vetor = [cliente_id]
year = ['2023']
mes = 11

for i in range(len(vetor)):
    for z in range(len(year)):
        print("Iniciando sumarização das entradas diárias.")
        consulta = '''SELECT 
        -- Reclamações registradas por dia no mês atual
        COUNT(CASE WHEN (day(dataAbertura) = 1  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia1,
        COUNT(CASE WHEN (day(dataAbertura) = 2  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia2,
        COUNT(CASE WHEN (day(dataAbertura) = 3  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia3,
        COUNT(CASE WHEN (day(dataAbertura) = 4  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia4,
        COUNT(CASE WHEN (day(dataAbertura) = 5 and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia5,
        COUNT(CASE WHEN (day(dataAbertura) = 6  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia6,
        COUNT(CASE WHEN (day(dataAbertura) = 7  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia7,
        COUNT(CASE WHEN (day(dataAbertura) = 8  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia8,
        COUNT(CASE WHEN (day(dataAbertura) = 9  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia9,
        COUNT(CASE WHEN (day(dataAbertura) = 10  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia10,
        COUNT(CASE WHEN (day(dataAbertura) = 11  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia11,
        COUNT(CASE WHEN (day(dataAbertura) = 12  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia12,
        COUNT(CASE WHEN (day(dataAbertura) = 13  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia13,
        COUNT(CASE WHEN (day(dataAbertura) = 14  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia14,
        COUNT(CASE WHEN (day(dataAbertura) = 15  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia15,
        COUNT(CASE WHEN (day(dataAbertura) = 16  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia16,
        COUNT(CASE WHEN (day(dataAbertura) = 17  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia17,
        COUNT(CASE WHEN (day(dataAbertura) = 18  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia18,
        COUNT(CASE WHEN (day(dataAbertura) = 19  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia19,
        COUNT(CASE WHEN (day(dataAbertura) = 20  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia20,
        COUNT(CASE WHEN (day(dataAbertura) = 21  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia21,
        COUNT(CASE WHEN (day(dataAbertura) = 22  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia22,
        COUNT(CASE WHEN (day(dataAbertura) = 23  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia23,
        COUNT(CASE WHEN (day(dataAbertura) = 24  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia24,
        COUNT(CASE WHEN (day(dataAbertura) = 25  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia25,
        COUNT(CASE WHEN (day(dataAbertura) = 26  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia26,
        COUNT(CASE WHEN (day(dataAbertura) = 27  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia27,
        COUNT(CASE WHEN (day(dataAbertura) = 28  and (MONTH(dataAbertura) = {3} and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia28,
        COUNT(CASE WHEN (day(dataAbertura) = 29  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia29,
        COUNT(CASE WHEN (day(dataAbertura) = 30  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia30,
        COUNT(CASE WHEN (day(dataAbertura) = 31  and (MONTH(dataAbertura) = {3}  and  YEAR(dataAbertura)  = {0}))THEN 1 END) dia31
        from reclamacoes
        WHERE clienteId {2} {1}'''.format(year[z], vetor[i], sinal[i], mes)

        mycursor.execute(consulta)
        myresult = mycursor.fetchall()
        count = -1
        periodo = 180
        clienteId = cliente_id
        ano = year[z]
        mes = mes
        dia1 = myresult[0][0]
        dia2 = myresult[0][1]
        dia3 = myresult[0][2]
        dia4 = myresult[0][3]
        dia5 = myresult[0][4]
        dia6 = myresult[0][5]
        dia7 = myresult[0][6]
        dia8 = myresult[0][7]
        dia9 = myresult[0][8]
        dia10 = myresult[0][9]
        dia11 = myresult[0][10]
        dia12 = myresult[0][11]
        dia13 = myresult[0][12]
        dia14 = myresult[0][13]
        dia15 = myresult[0][14]
        dia16 = myresult[0][15]
        dia17 = myresult[0][16]
        dia18 = myresult[0][17]
        dia19 = myresult[0][18]
        dia20 = myresult[0][19]
        dia21 = myresult[0][20]
        dia22 = myresult[0][21]
        dia23 = myresult[0][22]
        dia24 = myresult[0][23]
        dia25 = myresult[0][24]
        dia26 = myresult[0][25]
        dia27 = myresult[0][26]
        dia28 = myresult[0][27]
        dia29 = myresult[0][28]
        dia30 = myresult[0][29]
        dia31 = myresult[0][30]

        sql7 = "INSERT INTO tblentradasdiaria(dia1,dia2,dia3,dia4,dia5,dia6,dia7,dia8,dia9,dia10,dia11,dia12,dia13,dia14,dia15,dia16,dia17,dia18,dia19,dia20,dia21,dia22,dia23,dia24,dia25,dia26,dia27,dia28,dia29,dia30,dia31,mes,clienteId,ano) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val7 = [(dia1), (dia2), (dia3), (dia4), (dia5), (dia6), (dia7), (dia8), (dia9), (dia10), (dia11), (dia12),
                (dia13), (dia14), (dia15), (dia16), (dia17), (dia18), (dia19), (dia20), (dia21), (dia22), (dia23),
                (dia24), (dia25), (dia26), (dia27), (dia28), (dia29), (dia30), (dia31), (mes), (clienteId), (ano)]
        mycursor.execute(sql7, val7)

print("Sumarização completa.")
# Encerra conexão com o banco de dados, se houver.
if mydb.is_connected():
    mycursor.close()
    mydb.close()
    print("Conexão com o banco de dados foi encerrada")


