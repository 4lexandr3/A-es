# Carregar dados históricos da Bovespa em tabela para análise

import sqlite3 as lite
import sys

""""
select cd_acao, sum(ic_1_00_pc), sum(ic_1_50_pc), sum(ic_2_00_pc), sum(vr_volume), sum(vr_result_1_00_pc), sum(vr_result_1_50_pc), sum(vr_result_2_00_pc)
from hist_dados
where dt_pregao in (select distinct dt_pregao from hist_dados
                    order by 1 DESC	limit 20)
group by cd_acao
having sum(ic_1_00_pc) >= 16
order by 5 DESC

SELECT cd_acao, dt_pregao, vr_fechamento as cotacao, vr_volume, pc_variacao as percent, vr_maximo_dia as vr_max, vr_minimo_dia as vr_min, pc_maximo_dia as pc_max, pc_minimo_dia as pc_min
     , case ic_1_00_pc when 1 then 'sim' else '' end _1_00
     , case ic_1_50_pc when 1 then 'sim' else '' end _1_50
     , case ic_2_00_pc when 1 then 'sim' else '' end _2_00
	 , vr_result_1_00_pc as vr_1_00, vr_result_1_50_pc as vr_1_50, vr_result_2_00_pc as vr_2_00
FROM hist_dados
WHERE cd_acao LIKE 'PRIO3%'
ORDER BY 2 DESC , 1 DESC
"""


con = None

try:
    con = lite.connect('acoes.db')
    cur = con.cursor()
    cur.execute('SELECT SQLITE_VERSION()')
    data = cur.fetchone()[0]

    print("SQLite version: {}".format(data))
    print("Processando...")

    sqlcreate = 'CREATE TABLE IF NOT EXISTS hist_dados ' \
                '(cd_acao VARCHAR(12), ' \
                'dt_pregao VARCHAR(10), ' \
                'vr_fechamento FLOAT, ' \
                'vr_volume INTEGER, ' \
                'pc_variacao FLOAT, ' \
                'vr_maximo_dia FLOAT, ' \
                'vr_minimo_dia FLOAT, ' \
                'pc_maximo_dia FLOAT, ' \
                'pc_minimo_dia FLOAT, ' \
                'ic_1_00_pc INTEGER, ' \
                'ic_1_50_pc INTEGER, ' \
                'ic_2_00_pc INTEGER, ' \
                'vr_result_1_00_pc FLOAT, ' \
                'vr_result_1_50_pc FLOAT, ' \
                'vr_result_2_00_pc FLOAT, ' \
                'PRIMARY KEY (cd_acao, dt_pregao))'

    cur.execute(sqlcreate)

except lite.Error as e:
    print("Error {}:".format(e.args[0]))
    sys.exit(1)

f = open("COTAHIST_A2019.TXT", "r")
if f.mode == 'r':
    lista1 = f.readlines()

f = open("COTAHIST_A2020.TXT", "r")
if f.mode == 'r':
    lista2 = f.readlines()

lista = lista1 + lista2

x = 0
listaAux = []
for i in lista:
    listaAux.append(lista[x][0:2] + lista[x][12:24] + lista[x][2:12] + lista[x][24:245])
    x+=1

listaAux.sort(reverse=True)
x = 0
vrInvestimentoPadrao = 20000
sql_insert = 'INSERT INTO hist_dados VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

for i in listaAux:
    TIPREG = int(listaAux[x][0:2])
    if TIPREG == 1:
        TIPREGprox = int(listaAux[x+1][0:2])
    CODBDI = listaAux[x][22:24]
    if TIPREG == 1 and CODBDI == '02':
        cdAcao = listaAux[x][2:14]
        dtPregao = listaAux[x][14:18] + "/" + listaAux[x][18:20] + "/" + listaAux[x][20:22]
        vrFechamento = float(listaAux[x][108:121]) / 100
        if TIPREGprox == 0:
            vrFechamentoAnt = 1
        else:
            vrFechamentoAnt = float(listaAux[x+1][108:121]) / 100
        vrVolume = float(listaAux[x][170:188]) / 100
        pcVariacao = round(((vrFechamento / vrFechamentoAnt) - 1) * 100, 2)
        vrMaximoDia = float(listaAux[x][69:82]) / 100
        vrMinimoDia = float(listaAux[x][82:95]) / 100
        pcMaximoDia = round(((vrMaximoDia / vrFechamentoAnt) - 1) * 100, 2)
        pcMinimoDia = round(((vrMinimoDia / vrFechamentoAnt) - 1) * 100, 2)
        if pcMaximoDia > 1: ic_1_00_pc = 1
        else:               ic_1_00_pc = 0

        if pcMaximoDia > 1.5: ic_1_50_pc = 1
        else:                 ic_1_50_pc = 0

        if pcMaximoDia > 2: ic_2_00_pc = 1
        else:               ic_2_00_pc = 0

        if ic_1_00_pc == 1: vrResult_1_00_pc = vrInvestimentoPadrao * 0.01
        else:               vrResult_1_00_pc = (vrInvestimentoPadrao * pcVariacao) / 100

        if ic_1_50_pc == 1: vrResult_1_50_pc = vrInvestimentoPadrao * 0.015
        else:               vrResult_1_50_pc = (vrInvestimentoPadrao * pcVariacao) / 100

        if ic_2_00_pc == 1: vrResult_2_00_pc = vrInvestimentoPadrao * 0.02
        else:               vrResult_2_00_pc = (vrInvestimentoPadrao * pcVariacao) / 100

        registroAcoes = [cdAcao,
                         dtPregao,
                         vrFechamento,
                         vrVolume,
                         pcVariacao,
                         vrMaximoDia,
                         vrMinimoDia,
                         pcMaximoDia,
                         pcMinimoDia,
                         ic_1_00_pc,
                         ic_1_50_pc,
                         ic_2_00_pc,
                         vrResult_1_00_pc,
                         vrResult_1_50_pc,
                         vrResult_2_00_pc]

        try:
            cur.execute(sql_insert, registroAcoes)

        except lite.Error as e:
            con.commit()
            print("Error {}:".format(e.args[0]))
            print(registroAcoes)
            sys.exit(1)

    x+=1

if con:
    con.commit()
    con.close()

print("Carga concluída com sucesso!")
