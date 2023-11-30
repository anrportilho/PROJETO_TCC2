import funcoes.conn_api
import funcoes.conn_bd
import json
from datetime import datetime
from tqdm import tqdm
import requests

# Conecta com banco de dados
# ----------------------------
mydb = funcoes.conn_bd.connect()
mycursor = mydb.cursor()
# ----------------------------

mydata = funcoes.conn_api.datas()
mycursor.execute("SELECT cliente_id, cod, chave, cpf FROM cod_empresa")
myresult = mycursor.fetchall()
print(len(myresult))
protocolo = []

for i in range(len(myresult)):
    cliente_id, cod, chave, cpf = myresult[i]
    mylist = funcoes.conn_api.autenticacao_detalhe(cliente_id,cod,chave,cpf)
    CLIENT_ID = cliente_id
    mycursor.execute(
         "SELECT protocolo as protocolo, situacaoDescricao as situacao FROM reclamacoes e WHERE e.protocolo NOT IN (SELECT num_prot FROM detalhereclamacao) AND e.clienteId ='" + str(
            CLIENT_ID) + "'")
    myresult2 = mycursor.fetchall()
    for a in range(len(myresult2)):
        prot, situacao = myresult2[a]
        protocolo.append(prot)
    protocolo.sort()
    # ---------------------------------------------------------------------------------
    quantidade = len(protocolo)

    for b in tqdm(range(0, quantidade)):
        temp = protocolo[b]
        url_listar = 'http://api.consumidor.gov.br/api/servico/reclamacao/' + str(temp) + '/detalhe'
        request = requests.get(url_listar, headers=mylist[0], params=mylist[1])
        todo = json.loads(request.content)
        listOfValues = todo.values()
        listOfValues = list(listOfValues)
        # pega todos os dados de retorno da API
        reclamacao = listOfValues[1]
        try:
            num_prot = reclamacao['protocolo']
            situacaoCodigo = reclamacao['situacao']['codigo']
            situacao = reclamacao['situacao']['descricao']
            meioConsumo = reclamacao['meioConsumo']['descricao']
            dt_Prazo = datetime.strptime(reclamacao['prazo'], "%d/%m/%Y %H:%M:%S")
            dtPrazo = dt_Prazo.strftime('%Y-%m-%d %H:%M:%S')
            prazo = dtPrazo
            prazoManifest = datetime.strptime(reclamacao['prazoManifestacao'], "%d/%m/%Y %H:%M:%S")
            dtPrazoManifest = prazoManifest.strftime('%Y-%m-%d %H:%M:%S')
            prazoManifestacao = dtPrazoManifest
            nomeCampoPrazo = reclamacao['nomeCampoPrazo']
            dt_abert = datetime.strptime(reclamacao['dataAbertura'], "%d/%m/%Y %H:%M:%S")
            dataAbert = dt_abert.strftime('%Y-%m-%d %H:%M:%S')
            dataAbertura = dataAbert
            area = reclamacao['area']['descricao']
            assunto = reclamacao['assunto']['descricao']
            problema = reclamacao['problema']['descricao']
            reclamante = reclamacao['reclamante']['nome']
            reclamanteEstado = reclamacao['reclamante']['estado']
            reclamanteCidade = reclamacao['reclamante']['cidade']
            reclamanteLogradouro = reclamacao['reclamante']['logradouro']
            reclamanteBairro = reclamacao['reclamante']['bairro']
            reclamantenNumeroComplemento = reclamacao['reclamante']['numeroComplemento']
            reclamanteDataNascimento = reclamacao['reclamante']['dataNascimento']
            reclamanteDocumento = reclamacao['reclamante']['documento']
            reclamanteCep = reclamacao['reclamante']['cep']
            reclamanteEmail = reclamacao['reclamante']['emails']
            mycursor = mydb.cursor()
            for i in range(len(reclamanteEmail)):
                reclamanteEmail = reclamacao['reclamante']['emails'][i]['email']
                chaveEmail = str(num_prot)+str(reclamanteEmail)
                sql = "INSERT IGNORE INTO emailsreclamante(num_prot,reclamanteEmail,chave) values(%s,%s,%s)"
                val = (num_prot, reclamanteEmail,chaveEmail)
                mycursor.execute(sql, val)
                mydb.commit()
            reclamanteTelefones = reclamacao['reclamante']['telefones']
            mycursor = mydb.cursor()
            for i in range(len(reclamanteTelefones)):
                reclamanteTelefonesddd = reclamacao['reclamante']['telefones'][i]['ddd']
                reclamanteTelefonesNumero = reclamacao['reclamante']['telefones'][i]['telefone']
                chaveTelefone = str(num_prot) + str(reclamanteTelefonesNumero)
                sql = "INSERT IGNORE INTO reclamantetelefones(num_prot,reclamanteTelefonesddd,reclamanteTelefonesNumero,chave) values(%s,%s,%s,%s)"
                val = (num_prot, reclamanteTelefonesddd, reclamanteTelefonesNumero, chaveTelefone)
                mycursor.execute(sql, val)
                mydb.commit()
            reclamadoNumeroIdentificacao = reclamacao['reclamado']['numeroIdentificacao']
            reclamadoNome = reclamacao['reclamado']['nome']
            reclamadoNomeFantasia = reclamacao['reclamado']['nomeFantasia']
            reclamadoEstado = reclamacao['reclamado']['estado']
            reclamadoCidade = reclamacao['reclamado']['cidade']
            reclamadoLogradouro = reclamacao['reclamado']['logradouro']
            reclamadoBairro = reclamacao['reclamado']['bairro']
            reclamadoNumeroComplemento = reclamacao['reclamado']['numeroComplemento']
            reclamadoDocumento = reclamacao['reclamado']['documento']
            reclamadoCep = reclamacao['reclamado']['cep']
            mediadorNumeroIdentificacao = reclamacao['mediador']['numeroIdentificacao']
            mediadorNome = reclamacao['mediador']['nome']
            mediadorNomeFantasia = reclamacao['mediador']['nomeFantasia']
            mediadorEstado = reclamacao['mediador']['estado']
            mediadorCidade = reclamacao['mediador']['cidade']
            mediadorLogradouro = reclamacao['mediador']['logradouro']
            mediadorBairro = reclamacao['mediador']['bairro']
            mediadorNumeroComplemento = reclamacao['mediador']['numeroComplemento']
            mediadorDocumento = reclamacao['mediador']['documento']
            mediadorCep = reclamacao['mediador']['cep']
            tramite = reclamacao['tramites']
            textos = tramite[0]
            reclamacaoReclamante = textos['textos'][0]['texto']
            pedidoReclamante = textos['textos'][1]['texto']
            mycursor = mydb.cursor()
            sql = "INSERT IGNORE INTO detalhereclamacao (num_prot,situacaoCodigo,situacao,meioConsumo,prazo,prazoManifestacao,nomeCampoPrazo,dataAbertura,area,assunto,problema,reclamante,reclamanteEstado,reclamanteCidade,reclamanteLogradouro,reclamanteBairro,reclamantenNumeroComplemento,reclamanteDataNascimento,reclamanteDocumento,reclamanteCep,reclamadoNumeroIdentificacao,reclamadoNome,reclamadoNomeFantasia,reclamadoEstado,reclamadoCidade,reclamadoLogradouro,reclamadoBairro,reclamadoNumeroComplemento,reclamadoDocumento,reclamadoCep,mediadorNumeroIdentificacao,mediadorNome,mediadorNomeFantasia,mediadorEstado,mediadorCidade,mediadorLogradouro,mediadorBairro,mediadorNumeroComplemento,mediadorDocumento,mediadorCep,reclamacaoReclamante,pedidoReclamante) values(%s,%s,%s,%s,%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,	%s,%s) ON DUPLICATE KEY UPDATE situacaoCodigo =%s ,situacao =%s ,prazo = %s, prazoManifestacao = %s, nomeCampoPrazo = %s"
            val = (
                num_prot, situacaoCodigo, situacao, meioConsumo, prazo, prazoManifestacao, nomeCampoPrazo, dataAbertura,
                area,
                assunto, problema, reclamante, reclamanteEstado, reclamanteCidade, reclamanteLogradouro,
                reclamanteBairro,
                reclamantenNumeroComplemento, reclamanteDataNascimento, reclamanteDocumento, reclamanteCep,
                reclamadoNumeroIdentificacao, reclamadoNome, reclamadoNomeFantasia, reclamadoEstado, reclamadoCidade,
                reclamadoLogradouro, reclamadoBairro, reclamadoNumeroComplemento, reclamadoDocumento, reclamadoCep,
                mediadorNumeroIdentificacao, mediadorNome, mediadorNomeFantasia, mediadorEstado, mediadorCidade,
                mediadorLogradouro,
                mediadorBairro, mediadorNumeroComplemento, mediadorDocumento, mediadorCep, reclamacaoReclamante,
                pedidoReclamante,situacaoCodigo, situacao, prazo, prazoManifestacao, nomeCampoPrazo)
            mycursor.execute(sql, val)
            mydb.commit()
            mycursor = mydb.cursor()
            tramiteId = 0

            for i in range(len(tramite)):
                tramiteCodigo = reclamacao['tramites'][i]['codigo']
                tramiteTipoCodigo = reclamacao['tramites'][i]['tipo']['codigo']
                tramiteTipoDescricao = reclamacao['tramites'][i]['tipo']['descricao']
                descricaoTexto = tramiteTipoDescricao
                codigoTramite = tramiteTipoCodigo
                Tram_Dta = datetime.strptime(reclamacao['tramites'][i]['data'], "%d/%m/%Y %H:%M:%S")
                TramitDt = Tram_Dta.strftime('%Y-%m-%d %H:%M:%S')
                tramiteData = TramitDt
                tramiteAutor = reclamacao['tramites'][i]['autor']
                tramiteProtocoloReclamacao = reclamacao['tramites'][i]['protocoloReclamacao']
                chave_tramites = str(tramiteProtocoloReclamacao) + str(tramiteTipoCodigo) + str(tramiteData)
                chaveTramites = chave_tramites.replace("-", "").replace(":", "").replace(" ", "")
                sql = "INSERT IGNORE INTO tramites (tramiteCodigo,tramiteTipoCodigo,tramiteTipoDescricao,tramiteData,tramiteAutor,tramiteProtocoloReclamacao,chavetramite) values(%s,%s,%s,%s,%s,%s,%s)"
                val = (
                    tramiteCodigo, tramiteTipoCodigo, tramiteTipoDescricao, tramiteData, tramiteAutor,
                    tramiteProtocoloReclamacao,chaveTramites)
                mycursor.execute(sql, val)
                total = range(len(tramite[i]['textos']))

                for x in total:
                    tramiteTextos = tramite[i]['textos'][x]['texto']
                    tramiteTipoCodigo = reclamacao['tramites'][i]['textos'][x]['tipo']['codigo']
                    tramiteTipoDescricao = reclamacao['tramites'][i]['textos'][x]['tipo']['descricao']
                    chave_Texto_tramites = str(codigoTramite)+str(tramiteProtocoloReclamacao) + str(tramiteTipoCodigo) + str(tramiteData)
                    chaveTextoTramites = chave_Texto_tramites.replace("-", "").replace(":", "").replace(" ", "")
                    mycursor = mydb.cursor()
                    sql = "INSERT IGNORE INTO textostramites (tramiteTextos,tramiteTipoCodigo,tramiteTipoDescricao,tramiteProtocoloReclamacao,tramiteData,descricaoTexto,tramiteAutor,codigoTramite,clienteId,chavetextotramites) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    val = (
                        tramiteTextos, tramiteTipoCodigo, tramiteTipoDescricao, tramiteProtocoloReclamacao, tramiteData,
                        descricaoTexto,tramiteAutor, codigoTramite, CLIENT_ID,chaveTextoTramites)
                    mycursor.execute(sql, val)
            mydb.commit()

        except:
            print("Deu erro no protocolo: " + str(temp))
            mycursor = mydb.cursor()
            sql = "INSERT INTO logerro (protocolo,clienteId,datinicio,datafim) values(%s,%s,%s,%s)"
            val = (temp, CLIENT_ID, mydata[0], mydata[1])
            mycursor.execute(sql, val)
            mydb.commit()

mydb.commit()
# Encerra conexão com o banco de dados, se houver.
if mydb.is_connected():
    mycursor.close()
    mydb.close()
    print("Conexão ao MySQL foi encerrada")




