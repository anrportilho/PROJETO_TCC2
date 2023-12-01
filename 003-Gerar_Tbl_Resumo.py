import funcoes.conn_bd

# Conecta com banco de dados
# ----------------------------
mydb = funcoes.conn_bd.connect()
mycursor = mydb.cursor()
# ----------------------------

mycursor.execute("SELECT cliente_id, cod, chave, cpf FROM cod_empresa")
myresult = mycursor.fetchall()
cliente_id = myresult[0][0]

mycursor = mydb.cursor()
sql0 = "TRUNCATE TABLE tblresumo"
mycursor.execute(sql0)

mycursor = mydb.cursor()
sql0 = "TRUNCATE TABLE tblentradasmes"
mycursor.execute(sql0)

mycursor = mydb.cursor()
sql0 = "TRUNCATE TABLE tblfinalizadasmes"
mycursor.execute(sql0)

mycursor = mydb.cursor()
sql0 = "TRUNCATE TABLE tblemanalisefornecedor"
mycursor.execute(sql0)

mycursor = mydb.cursor()
sql0 = "TRUNCATE TABLE tblemanalisegestor"
mycursor.execute(sql0)

mycursor = mydb.cursor()
sql0 = "TRUNCATE TABLE tblfinalizadasavaliadasmes"
mycursor.execute(sql0)

sinal = ['=']
vetor = [cliente_id]
year = ['2023']

for i in range(len(vetor)):
    print("Iniciando sumarização dos dados")
    for z in range(len(year)):
        consulta = '''SELECT 
        COUNT(CASE WHEN (situacaoCodigo = 1) THEN 1 END) emAberto,
        COUNT(CASE WHEN (situacaoCodigo = 3 and YEAR(prazo) = {0}) THEN 1 END) emAnaliseFornecedor,
        COUNT(CASE WHEN (situacaoCodigo = 2 and YEAR(prazo) = {0}) THEN 1 END) emAnaliseGestor,
        COUNT(CASE WHEN (situacaoCodigo = 4 and YEAR(prazo) = {0}) THEN 1 END) respondida,

        COUNT(CASE WHEN (YEAR(dataAbertura)  = {0}) THEN 1 END) totalEntradas,

        COUNT(CASE WHEN (situacaoCodigo = 9 and YEAR(prazo) = {0}) THEN 1 END) encerrada,
        COUNT(CASE WHEN (situacaoCodigo = 6 and YEAR(prazo) = {0}) THEN 1 END) finalizadaAvaliada,
        COUNT(CASE WHEN (situacaoCodigo = 7 and YEAR(prazo) = {0}) THEN 1 END) finalizadaNaoAvaliada,
        COUNT(CASE WHEN (situacaoCodigo = 8 and YEAR(prazo) = {0}) THEN 1 END) cancelada,

        -- Reclamações cadastradas por mês
        COUNT(CASE WHEN (MONTH(dataAbertura) = 1  and  YEAR(dataAbertura)  = {0}) THEN 1 END) e1,
        COUNT(CASE WHEN (MONTH(dataAbertura) = 2  and  YEAR(dataAbertura)  = {0}) THEN 1 END) e2,
        COUNT(CASE WHEN (MONTH(dataAbertura) = 3  and  YEAR(dataAbertura)  = {0}) THEN 1 END) e3,
        COUNT(CASE WHEN (MONTH(dataAbertura) = 4  and  YEAR(dataAbertura)  = {0}) THEN 1 END) e4,
        COUNT(CASE WHEN (MONTH(dataAbertura) = 5  and  YEAR(dataAbertura)  = {0}) THEN 1 END) e5,
        COUNT(CASE WHEN (MONTH(dataAbertura) = 6  and  YEAR(dataAbertura)  = {0}) THEN 1 END) e6,
        COUNT(CASE WHEN (MONTH(dataAbertura) = 7  and  YEAR(dataAbertura)  = {0}) THEN 1 END) e7,
        COUNT(CASE WHEN (MONTH(dataAbertura) = 8  and  YEAR(dataAbertura)  = {0}) THEN 1 END) e8,
        COUNT(CASE WHEN (MONTH(dataAbertura) = 9  and  YEAR(dataAbertura)  = {0}) THEN 1 END) e9,
        COUNT(CASE WHEN (MONTH(dataAbertura) = 10 and  YEAR(dataAbertura)  = {0}) THEN 1 END) e10,
        COUNT(CASE WHEN (MONTH(dataAbertura) = 11 and  YEAR(dataAbertura)  = {0}) THEN 1 END) e11,
        COUNT(CASE WHEN (MONTH(dataAbertura) = 12 and  YEAR(dataAbertura)  = {0}) THEN 1 END) e12,

        -- Reclamações finalizadas por mês
        COUNT(CASE WHEN (situacaoCodigo = 6 AND MONTH(prazo) = 1 and YEAR(prazo) = {0}) OR (situacaoCodigo = 7 AND MONTH(prazo) = 1 and YEAR(prazo) = {0}) THEN 1 END) f1,
        COUNT(CASE WHEN (situacaoCodigo = 6 AND MONTH(prazo) = 2 and YEAR(prazo) = {0}) OR (situacaoCodigo = 7 AND MONTH(prazo) = 2 and YEAR(prazo) = {0}) THEN 1 END) f2,
        COUNT(CASE WHEN (situacaoCodigo = 6 AND MONTH(prazo) = 3 and YEAR(prazo) = {0}) OR (situacaoCodigo = 7 AND MONTH(prazo) = 3 and YEAR(prazo) = {0}) THEN 1 END) f3,
        COUNT(CASE WHEN (situacaoCodigo = 6 AND MONTH(prazo) = 4 and YEAR(prazo) = {0}) OR (situacaoCodigo = 7 AND MONTH(prazo) = 4 and YEAR(prazo) = {0}) THEN 1 END) f4,
        COUNT(CASE WHEN (situacaoCodigo = 6 AND MONTH(prazo) = 5 and YEAR(prazo) = {0}) OR (situacaoCodigo = 7 AND MONTH(prazo) = 5 and YEAR(prazo) = {0}) THEN 1 END) f5,
        COUNT(CASE WHEN (situacaoCodigo = 6 AND MONTH(prazo) = 6 and YEAR(prazo) = {0}) OR (situacaoCodigo = 7 AND MONTH(prazo) = 6 and YEAR(prazo) = {0}) THEN 1 END) f6,
        COUNT(CASE WHEN (situacaoCodigo = 6 AND MONTH(prazo) = 7 and YEAR(prazo) = {0}) OR (situacaoCodigo = 7 AND MONTH(prazo) = 7 and YEAR(prazo) = {0}) THEN 1 END) f7,
        COUNT(CASE WHEN (situacaoCodigo = 6 AND MONTH(prazo) = 8 and YEAR(prazo) = {0}) OR (situacaoCodigo = 7 AND MONTH(prazo) = 8 and YEAR(prazo) = {0}) THEN 1 END) f8,
        COUNT(CASE WHEN (situacaoCodigo = 6 AND MONTH(prazo) = 9 and YEAR(prazo) = {0}) OR (situacaoCodigo = 7 AND MONTH(prazo) = 9 and YEAR(prazo) = {0}) THEN 1 END) f9,
        COUNT(CASE WHEN (situacaoCodigo = 6 AND MONTH(prazo) = 10 and YEAR(prazo) = {0}) OR (situacaoCodigo = 7 AND MONTH(prazo) = 10 and YEAR(prazo) = {0}) THEN 1 END) f10,
        COUNT(CASE WHEN (situacaoCodigo = 6 AND MONTH(prazo) = 11 and YEAR(prazo) = {0}) OR (situacaoCodigo = 7 AND MONTH(prazo) = 11 and YEAR(prazo) = {0}) THEN 1 END) f11,
        COUNT(CASE WHEN (situacaoCodigo = 6 AND MONTH(prazo) = 12 and YEAR(prazo) = {0}) OR (situacaoCodigo = 7 AND MONTH(prazo) = 12 and YEAR(prazo) = {0}) THEN 1 END) f12,

        -- EM ANALISE PELO FORNECEDOR
        COUNT(CASE WHEN (situacaoCodigo = 3 AND prazo = DATE(CURRENT_DATE)) THEN 1 END) venc0,
        COUNT(CASE WHEN (situacaoCodigo = 3 AND prazo = DATE(CURRENT_DATE + interval 1 DAY)) THEN 1 END) fo1,
        COUNT(CASE WHEN (situacaoCodigo = 3 AND prazo = DATE(CURRENT_DATE + interval 2 DAY)) THEN 1 END) fo2,
        COUNT(CASE WHEN (situacaoCodigo = 3 AND prazo = DATE(CURRENT_DATE + interval 3 DAY)) THEN 1 END) fo3,
        COUNT(CASE WHEN (situacaoCodigo = 3 AND prazo = DATE(CURRENT_DATE + interval 4 DAY)) THEN 1 END) fo4,
        COUNT(CASE WHEN (situacaoCodigo = 3 AND prazo = DATE(CURRENT_DATE + interval 5 DAY)) THEN 1 END) fo5,
        COUNT(CASE WHEN (situacaoCodigo = 3 AND prazo = DATE(CURRENT_DATE + interval 6 DAY)) THEN 1 END) fo6,
        COUNT(CASE WHEN (situacaoCodigo = 3 AND prazo = DATE(CURRENT_DATE + interval 7 DAY)) THEN 1 END) fo7,
        COUNT(CASE WHEN (situacaoCodigo = 3 AND prazo = DATE(CURRENT_DATE + interval 8 DAY)) THEN 1 END) fo8,
        COUNT(CASE WHEN (situacaoCodigo = 3 AND prazo = DATE(CURRENT_DATE + interval 9 DAY)) THEN 1 END) fo9,
        COUNT(CASE WHEN (situacaoCodigo = 3 AND prazo = DATE(CURRENT_DATE + interval 10 DAY)) THEN 1 END) fo10,
        COUNT(CASE WHEN (situacaoCodigo = 3 AND prazo = DATE(CURRENT_DATE + interval 11 DAY)) THEN 1 END) fo11,
        COUNT(CASE WHEN (situacaoCodigo = 3 AND prazo = DATE(CURRENT_DATE + interval 12 DAY)) THEN 1 END) fo12,

        -- EM ANALISE PELO GESTOR
        COUNT(CASE WHEN (situacaoCodigo = 2 AND prazo = DATE(CURRENT_DATE)) THEN 1 END) Fvenc0,
        COUNT(CASE WHEN (situacaoCodigo = 2 AND prazo = DATE(CURRENT_DATE + interval 1 DAY)) THEN 1 END) g1,
        COUNT(CASE WHEN (situacaoCodigo = 2 AND prazo = DATE(CURRENT_DATE + interval 2 DAY)) THEN 1 END) g2,
        COUNT(CASE WHEN (situacaoCodigo = 2 AND prazo = DATE(CURRENT_DATE + interval 3 DAY)) THEN 1 END) g3,
        COUNT(CASE WHEN (situacaoCodigo = 2 AND prazo = DATE(CURRENT_DATE + interval 4 DAY)) THEN 1 END) g4,
        COUNT(CASE WHEN (situacaoCodigo = 2 AND prazo = DATE(CURRENT_DATE + interval 5 DAY)) THEN 1 END) g5,
        COUNT(CASE WHEN (situacaoCodigo = 2 AND prazo = DATE(CURRENT_DATE + interval 6 DAY)) THEN 1 END) g6,
        COUNT(CASE WHEN (situacaoCodigo = 2 AND prazo = DATE(CURRENT_DATE + interval 7 DAY)) THEN 1 END) g7,
        COUNT(CASE WHEN (situacaoCodigo = 2 AND prazo = DATE(CURRENT_DATE + interval 8 DAY)) THEN 1 END) g8,
        COUNT(CASE WHEN (situacaoCodigo = 2 AND prazo = DATE(CURRENT_DATE + interval 9 DAY)) THEN 1 END) g9,
        COUNT(CASE WHEN (situacaoCodigo = 2 AND prazo = DATE(CURRENT_DATE + interval 10 DAY)) THEN 1 END) g10,
        COUNT(CASE WHEN (situacaoCodigo = 2 AND prazo = DATE(CURRENT_DATE + interval 11 DAY)) THEN 1 END) g11,
        COUNT(CASE WHEN (situacaoCodigo = 2 AND prazo = DATE(CURRENT_DATE + interval 11 DAY)) THEN 1 END) g12,

        -- FINALIZADAS AVALIADAS POR MÊS
        COUNT(CASE WHEN (MONTH(reclamacoes.prazo) = 1 and YEAR(prazo) = {0} and reclamacoes.situacaoCodigo = 6) THEN 1 END) fa1,
        COUNT(CASE WHEN (MONTH(reclamacoes.prazo) = 2 and YEAR(prazo) = {0} and reclamacoes.situacaoCodigo = 6) THEN 1 END) fa2,
        COUNT(CASE WHEN (MONTH(reclamacoes.prazo) = 3 and YEAR(prazo) = {0} and reclamacoes.situacaoCodigo = 6) THEN 1 END) fa3,
        COUNT(CASE WHEN (MONTH(reclamacoes.prazo) = 4 and YEAR(prazo) = {0} and reclamacoes.situacaoCodigo = 6) THEN 1 END) fa4,
        COUNT(CASE WHEN (MONTH(reclamacoes.prazo) = 5 and YEAR(prazo) = {0} and reclamacoes.situacaoCodigo = 6) THEN 1 END) fa5,
        COUNT(CASE WHEN (MONTH(reclamacoes.prazo) = 6 and YEAR(prazo) = {0} and reclamacoes.situacaoCodigo = 6) THEN 1 END) fa6,
        COUNT(CASE WHEN (MONTH(reclamacoes.prazo) = 7 and YEAR(prazo) = {0} and reclamacoes.situacaoCodigo = 6) THEN 1 END) fa7,
        COUNT(CASE WHEN (MONTH(reclamacoes.prazo) = 8 and YEAR(prazo) = {0} and reclamacoes.situacaoCodigo = 6) THEN 1 END) fa8,
        COUNT(CASE WHEN (MONTH(reclamacoes.prazo) = 9 and YEAR(prazo) = {0} and reclamacoes.situacaoCodigo = 6) THEN 1 END) fa9,
        COUNT(CASE WHEN (MONTH(reclamacoes.prazo) = 10 and YEAR(prazo) = {0} and reclamacoes.situacaoCodigo = 6) THEN 1 END) fa10,
        COUNT(CASE WHEN (MONTH(reclamacoes.prazo) = 11 and YEAR(prazo) = {0} and reclamacoes.situacaoCodigo = 6) THEN 1 END) fa11,
        COUNT(CASE WHEN (MONTH(reclamacoes.prazo) = 12 and YEAR(prazo) = {0} and reclamacoes.situacaoCodigo = 6) THEN 1 END) fa12

        from reclamacoes

        WHERE clienteId {2} {1}'''.format(year[z], vetor[i], sinal[i])

        mycursor.execute(consulta)
        myresult = mycursor.fetchall()
        count = -1
        emAberto = myresult[0][0]
        emAnaliseFornecedor = myresult[0][1]
        emAnaliseGestor = myresult[0][2]
        respondida = myresult[0][3]
        totalEntrada = myresult[0][4]
        encerrada = myresult[0][5]
        finalizadaAvaliada = myresult[0][6]
        finalizadaNaoAvaliada = myresult[0][7]
        cancelada = myresult[0][8]
        periodo = 180
        clienteId=cliente_id
        ano = year[z]

        e1 = myresult[0][9]
        e2 = myresult[0][10]
        e3 = myresult[0][11]
        e4 = myresult[0][12]
        e5 = myresult[0][13]
        e6 = myresult[0][14]
        e7 = myresult[0][15]
        e8 = myresult[0][16]
        e9 = myresult[0][17]
        e10 = myresult[0][18]
        e11 = myresult[0][19]
        e12 = myresult[0][20]

        f1 = myresult[0][21]
        f2 = myresult[0][22]
        f3 = myresult[0][23]
        f4 = myresult[0][24]
        f5 = myresult[0][25]
        f6 = myresult[0][26]
        f7 = myresult[0][27]
        f8 = myresult[0][28]
        f9 = myresult[0][29]
        f10 = myresult[0][30]
        f11 = myresult[0][31]
        f12 = myresult[0][32]

        fo1 = myresult[0][33]
        fo2 = myresult[0][34]
        fo3 = myresult[0][35]
        fo4 = myresult[0][36]
        fo5 = myresult[0][37]
        fo6 = myresult[0][38]
        fo7 = myresult[0][39]
        fo8 = myresult[0][40]
        fo9 = myresult[0][41]
        fo10 = myresult[0][42]
        fo11 = myresult[0][43]
        fo12 = myresult[0][44]

        g1 = myresult[0][45]
        g2 = myresult[0][46]
        g3 = myresult[0][47]
        g4 = myresult[0][48]
        g5 = myresult[0][49]
        g6 = myresult[0][50]
        g7 = myresult[0][51]
        g8 = myresult[0][52]
        g9 = myresult[0][53]
        g10 = myresult[0][54]
        g11 = myresult[0][55]
        g12 = myresult[0][56]

        fa1 = myresult[0][59]
        fa2 = myresult[0][60]
        fa3 = myresult[0][61]
        fa4 = myresult[0][62]
        fa5 = myresult[0][63]
        fa6 = myresult[0][64]
        fa7 = myresult[0][65]
        fa8 = myresult[0][66]
        fa9 = myresult[0][67]
        fa10 = myresult[0][68]
        fa11 = myresult[0][69]
        fa12 = myresult[0][67]

        sql1 = "INSERT INTO tblresumo(emAberto,emAnaliseFornecedor,emAnaliseGestor,respondida,totalEntrada,encerrada,finalizadaAvaliada,finalizadaNaoAvaliada,cancelada,periodo,clienteId,ano) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val1 = [(emAberto), (emAnaliseFornecedor), (emAnaliseGestor), (respondida), (totalEntrada), (encerrada),
                (finalizadaAvaliada), (finalizadaNaoAvaliada), (cancelada), (periodo), (clienteId), (ano)]
        mycursor.execute(sql1, val1)

        sql2 = "INSERT INTO tblentradasmes(e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,periodo,clienteId,ano) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val2 = [(e1), (e2), (e3), (e4), (e5), (e6), (e7), (e8), (e9), (e10), (e11), (e12), (periodo), (clienteId),
                (ano)]
        mycursor.execute(sql2, val2)

        sql3 = "INSERT INTO tblfinalizadasmes(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,periodo,clienteId,ano) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val3 = [(f1), (f2), (f3), (f4), (f5), (f6), (f7), (f8), (f9), (f10), (f11), (f12), (periodo), (clienteId),
                (ano)]
        mycursor.execute(sql3, val3)

        sql4 = "INSERT INTO tblemanalisefornecedor(fo1,fo2,fo3,fo4,fo5,fo6,fo7,fo8,fo9,fo10,fo11,fo12,periodo,clienteId,ano) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val4 = [(fo1), (fo2), (fo3), (fo4), (fo5), (fo6), (fo7), (fo8), (fo9), (fo10), (fo11), (fo12), (periodo),
                (clienteId), (ano)]
        mycursor.execute(sql4, val4)

        sql5 = "INSERT INTO tblemanalisegestor(g1,g2,g3,g4,g5,g6,g7,g8,g9,g10,g11,g12,periodo,clienteId,ano) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val5 = [(g1), (g2), (g3), (g4), (g5), (g6), (g7), (g8), (g9), (g10), (g11), (g12), (periodo), (clienteId),
                (ano)]
        mycursor.execute(sql5, val5)

        sql6 = "INSERT INTO tblfinalizadasavaliadasmes(fa1,fa2,fa3,fa4,fa5,fa6,fa7,fa8,fa9,fa10,fa11,fa12,periodo,clienteId,ano) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val6 = [(fa1), (fa2), (fa3), (fa4), (fa5), (fa6), (fa7), (fa8), (fa9), (fa10), (fa11), (fa12), (periodo),
                (clienteId), (ano)]
        mycursor.execute(sql6, val6)

print("Carga de dados completa.")

# Encerra conexão com o banco de dados, se houver.
if mydb.is_connected():
    mycursor.close()
    mydb.close()
    print("Conexão ao MySQL foi encerrada")


