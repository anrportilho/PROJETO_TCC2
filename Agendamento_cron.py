import schedule
import time
import sys
import subprocess

def listar_reclamacoes():
    subprocess.Popen([sys.executable, r'D:\AUTOMACOES\API\PROJETO_TCC2\001-Listar_reclamacoes.py'])

def atualizar_reclamacoes():
    subprocess.Popen([sys.executable, r'D:\AUTOMACOES\API\PROJETO_TCC2\002-atualizar_reclamacoes.py'])

def gerar_resumo():
    subprocess.Popen([sys.executable, r'D:\AUTOMACOES\API\PROJETO_TCC2\003Gerar_Tbl_Resumo.py'])

def gerar_indice_solucao():
    subprocess.Popen([sys.executable, r'D:\AUTOMACOES\API\PROJETO_TCC2\004-Gerar_Indice_solucao.py'])

def gerar_entradas_diarias():
    subprocess.Popen([sys.executable, r'D:\AUTOMACOES\API\PROJETO_TCC2\005_Gerar_Entradas_diarias.py'])

def problemas():
    subprocess.Popen([sys.executable, r'D:\AUTOMACOES\API\PROJETO_TCC2\006_Problemas.py'])

def ufs():
    subprocess.Popen([sys.executable, r'D:\AUTOMACOES\API\PROJETO_TCC2\007_UF.py'])


schedule.every().day.at("00:35").do(listar_reclamacoes)
schedule.every().day.at("00:40").do(atualizar_reclamacoes)
schedule.every().day.at("02:30").do(gerar_resumo)
schedule.every().day.at("02:31").do(gerar_indice_solucao)
schedule.every().day.at("02:32").do(gerar_entradas_diarias)
schedule.every().day.at("02:33").do(problemas)
schedule.every().day.at("02:34").do(ufs)

schedule.every().day.at("12:35").do(listar_reclamacoes)
schedule.every().day.at("12:40").do(atualizar_reclamacoes)
schedule.every().day.at("14:30").do(gerar_resumo)
schedule.every().day.at("14:31").do(gerar_indice_solucao)
schedule.every().day.at("14:32").do(gerar_entradas_diarias)
schedule.every().day.at("14:33").do(problemas)
schedule.every().day.at("14:34").do(ufs)

schedule.every().day.at("16:35").do(listar_reclamacoes)
schedule.every().day.at("16:40").do(atualizar_reclamacoes)
schedule.every().day.at("18:30").do(gerar_resumo)
schedule.every().day.at("18:31").do(gerar_indice_solucao)
schedule.every().day.at("18:32").do(gerar_entradas_diarias)
schedule.every().day.at("18:33").do(problemas)
schedule.every().day.at("18:34").do(ufs)


while True:
    schedule.run_pending()
    time.sleep(1)

all_jobs = schedule.get_jobs()
schedule.clear()
schedule.clear('daily-tasks')