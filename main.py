import pandas as pd
import numpy as np
from pandas import DataFrame
from sqlalchemy import create_engine
import pyodbc
import os
import datetime
import multiprocessing
import os
from dateutil.relativedelta import relativedelta

# pd.set_option('display.max_columns', None)
# engine = sa.create_engine('mssql+pyodbc://user:password@server/database')

PATH = r"C:\Users\hugo.baur\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfolio - Operação\FastBook_Otimizada_v2.xlsm"
# PATH = os.environ['USERPROFILE'] + r'\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfólio - Operação\FastBook_Otimizada_v2.xlsm"

CONTRATOS_WBC_CSV = r'C:\Users\hugo.baur\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfolio - Operação\teste\contratos_wbc.csv'
# C:\Users\hugo.baur\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfolio - Operação
CONTRATOS_CSV = r'C:\Users\hugo.baur\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfolio - Operação\teste\contratos.csv'
ACERTOS_CSV = r'C:\Users\hugo.baur\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfolio - Operação\teste\tabAcertos.csv'
JUROS_CSV = r'C:\Users\hugo.baur\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfolio - Operação\teste\tabJuros.csv'
INFLACAO_CSV = r'C:\Users\hugo.baur\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfolio - Operação\teste\tabInflacao.csv'
MKT_CSV = r'C:\Users\hugo.baur\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfolio - Operação\teste\tabMKT.csv'
MARCACAO_CSV = r'C:\Users\hugo.baur\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfolio - Operação\teste\tabMarcacao.csv'

# CONTRATOS_WBC_CSV = os.environ['USERPROFILE'] + r'\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfólio - Operação\contratos_wbc.csv'
#
# CONTRATOS_CSV = os.environ['USERPROFILE'] + r'\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfólio - Operação\contratos.csv'
# ACERTOS_CSV = os.environ['USERPROFILE'] + r'\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfólio - Operação\tabAcertos.csv'
# JUROS_CSV = os.environ['USERPROFILE'] + r'\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfólio - Operação\tabJuros.csv'
# INFLACAO_CSV = os.environ['USERPROFILE'] + r'\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfólio - Operação\tabInflacao.csv'
# MKT_CSV = os.environ['USERPROFILE'] + r'\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfólio - Operação\tabMKT.csv'
# MARCACAO_CSV = os.environ['USERPROFILE'] + r'\Matrix comercializadora de energia elétrica LTDA\Gestão de Projetos - Portfólio - Operação\tabMarcacao.csv'

credenciais = "mysql+mysqldb://sysmatrix@bd-prd-aplicacoes-matrix:x6E!yCzwIyPIu@bd-prd-aplicacoes-matrix.mysql.database.azure.com/portfolio"

def tab_marcacao(PATH):
    init_time = datetime.datetime.now()
    df_base = pd.read_excel(PATH,
                            sheet_name="Marcação",
                            skiprows=1  # ignora as primeiras 10 linhas do excel
                            )
    referencia_ccee()
    df_base = df_base.dropna(axis=1, how='all')
    df_base = df_base.dropna(axis=0, how='all')
    df_base = df_base.rename(columns=df_base.iloc[0]).drop(df_base.index[0])
    df_base = df_base.drop(df_base.columns[[0]], 1)
    df_base = df_base.dropna(axis=0, how='any')
    df_base = df_base.iloc[:-24]

    df_alta = pd.read_excel(PATH,
                            sheet_name="Marcação",
                            skiprows=35  # ignora as primeiras 10 linhas do excel
                            )
    df_alta = df_alta.dropna(axis=1, how='all')
    df_alta = df_alta.iloc[:-15]
    df_alta = df_alta.dropna(axis=0, how='all')
    df_alta = df_alta.dropna(axis=0, how='any')

    df_baixa = pd.read_excel(PATH,
                             sheet_name="Marcação",
                             skiprows=51  # ignora as primeiras 10 linhas do excel
                             )
    df_baixa = df_baixa.dropna(axis=1, how='all')
    df_baixa = df_baixa.rename(columns=df_baixa.iloc[0]).drop(df_baixa.index[0])
    df_baixa = df_baixa.dropna(axis=0, how='all')
    df_baixa = df_baixa.dropna(axis=0, how='any')

    df_baixa = df_baixa.melt(id_vars="CONVENTIONAL",
                             var_name="Date",
                             value_name="Value")
    df_alta = df_alta.melt(id_vars="CONVENTIONAL",
                           var_name="Date",
                           value_name="Value")
    df_base = df_base.melt(id_vars="CONVENTIONAL",
                           var_name="Date",
                           value_name="Value")

    df_base['Cenario'] = 'Base'
    df_alta['Cenario'] = 'Alta'
    df_baixa['Cenario'] = 'baixa'

    tabMarcacao_df = pd.concat([df_base, df_alta, df_baixa])
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_marcacao', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('tab_marcacao - exec_time  = {} seconds '.format(exec_time.seconds))
    return tabMarcacao_df


def referencia_ccee():
    from sqlalchemy import create_engine
    df = pd.read_excel(PATH,
                            sheet_name="Marcação",
                            skiprows=5  # ignora as primeiras 10 linhas do excel
                            )
    df = df.iloc[:1]
    df = df['Referência CCEE'].iloc[0]
    engine = create_engine(
        credenciais)
    list = []
    list.append(['ref_ccee', df])
    df_list = pd.DataFrame(list, columns=['parametro', 'valor'])
    df_list.to_sql(name='parametros', con=engine, if_exists='replace', index=False)


def tab_marcacao_excel(PATH):
    init_time = datetime.datetime.now()
    df_base = ler_excel(PATH, 'Marcação')
    df_base = df_base.iloc[2:]
    df_base = df_base.dropna(axis=1, how='all')
    df_base = df_base.dropna(axis=0, how='all')
    df_base = df_base.rename(columns=df_base.iloc[0]).drop(df_base.index[0])
    df_base = df_base.drop(df_base.columns[[0]], 1)
    df_base = df_base.dropna(axis=0, how='any')
    df_base = df_base.iloc[:-24]

    df_base = df_base.melt(id_vars="CONVENTIONAL",
                           var_name="Date",
                           value_name="Value")

    df_base['Cenario'] = 'Base'
    tabMarcacao_df = df_base
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_marcacao', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('tab_marcacao - exec_time  = {} seconds '.format(exec_time.seconds))
    return tabMarcacao_df


def tab_marcacao_to_azure(tabMarcacao_df):
    init_time = datetime.datetime.now()
    from sqlalchemy import create_engine
    engine = create_engine(
        credenciais)
    tabMarcacao_df.to_sql(name='tabmarcacao', con=engine, if_exists='replace')
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_marcacao_to_azure', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('tab_marcacao_to_azure - exec_time  = {} seconds '.format(exec_time.seconds))


def tempo_exec_to_azure(df):
    init_time = datetime.datetime.now()
    from sqlalchemy import create_engine
    engine = create_engine(
        credenciais)
    df.to_sql(name='tempo_exec', con=engine, if_exists='append')
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    # print ( 'tempo_exec_to_azure - exec_time  = {} seconds '.format( exec_time.seconds)  )


def tab_inflacao(PATH):
    init_time = datetime.datetime.now()
    df_inflacao = pd.read_excel(PATH,
                       sheet_name = "Inflação"
                       )
    df_inflacao = df_inflacao.dropna(axis=1, how='all')
    df_inflacao = df_inflacao.iloc[:-(373-7)]
    df_inflacao = df_inflacao.melt(id_vars=["INFLAÇÃO","Cenario"],
        var_name="Date",
        value_name="Value")
    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_inflacao',exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec,columns=['job','time'])
    tempo_exec_to_azure(df_tempo_exec)
    print ( 'tab_inflacao - exec_time  = {} seconds '.format( exec_time.seconds))
    return df_inflacao


def tab_inflacao_to_azure(df_inflacao):
    init_time = datetime.datetime.now()
    from sqlalchemy import create_engine
    engine = create_engine(credenciais)
    df_inflacao.to_sql(name='tabinflacao',con=engine,if_exists='replace')
    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_inflacao_to_azure',exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec,columns=['job','time'])
    tempo_exec_to_azure(df_tempo_exec)
    print ( 'tab_inflacao_to_azure - exec_time  = {} seconds '.format( exec_time.seconds)  )


def tab_inflacao_cenario(PATH):
    init_time = datetime.datetime.now()
    df_inflacao_cenario = pd.read_excel(PATH,
                       sheet_name = "Inflação"
                       )
    df_inflacao_cenario = df_inflacao_cenario.dropna(axis=1, how='all')
    df_inflacao_cenario = df_inflacao_cenario.iloc[11:]
    df_inflacao_cenario = df_inflacao_cenario.rename(columns=df_inflacao_cenario.iloc[0]).drop(df_inflacao_cenario.index[0])
    df_inflacao_cenario = df_inflacao_cenario.dropna(axis=1, how='all')
    df_inflacao_cenario.columns = ['CHAVE','CENARIO','MES','ANO','IPCA','REALIZADO_IPCA','MENSAL_IPCA','CORRECAO','IGPM','REALIZADO_IGPM','MENSAL_IGPM']
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_inflacao_cenario',exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec,columns=['job','time'])
    tempo_exec_to_azure(df_tempo_exec)
    print ('tab_inflacao_cenario - exec_time  = {} seconds '.format(exec_time.seconds))
    return df_inflacao_cenario


def tab_inflacao_cenario_to_azure(df_inflacao_cenario):
    init_time = datetime.datetime.now()
    from sqlalchemy import create_engine
    engine = create_engine(credenciais)
    df_inflacao_cenario.to_sql(name='tabinflacaocenario',con=engine,if_exists='replace')
    # LEMBRAR DE PREENCHER TODOS OS CAMPOS DA COLUNA CORREÇÃO!!!
    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_inflacao_cenario_to_azure',exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec,columns=['job','time'])
    tempo_exec_to_azure(df_tempo_exec)
    print ( 'tab_inflacao_cenario_to_azure - exec_time  = {} seconds '.format( exec_time.seconds)  )


def tab_mkt_mensal(tabMarcacao_df, df_inflacao_cenario, data_ini, data_fim):
    from datetime import timedelta, date
    init_time = datetime.datetime.now()
    from sqlalchemy import create_engine
    engine = create_engine(credenciais)

    data_ref_ccee = pd.read_sql(sql='Select valor from parametros where parametro = "ref_ccee" ', con=engine)

    row = []
    dates = pd.date_range(data_ini, data_fim, freq='MS').strftime("%Y/%m").tolist()
    for data in dates:
        data_split = data.split('/')
        data_inteira = data + "/01"
        data_inteira = data_inteira.replace('/', '-')
        # PLD ( Pegar PLD publicado quando houver, senão pegar o PLD Implicito

        #  PLD Publicado
        se_pld_publicado = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD SE/CO") & (
                    tabMarcacao_df["Date"] == data_inteira)]
        s_pld_publicado = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD SUL") & (
                    tabMarcacao_df["Date"] == data_inteira)]
        ne_pld_publicado = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD NE") & (
                    tabMarcacao_df["Date"] == data_inteira)]
        n_pld_publicado = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD NORTE") & (
                    tabMarcacao_df["Date"] == data_inteira)]

        # PLD

        if se_pld_publicado['Value'].values[0] != 0:
            se = se_pld_publicado
        else:
            se = tabMarcacao_df[
                (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD Implicito SE/CO") & (
                        tabMarcacao_df["Date"] == data_inteira)]

        if s_pld_publicado['Value'].values[0] != 0:
            s = s_pld_publicado
        else:
            s = tabMarcacao_df[
                (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD Implicito SUL") & (
                        tabMarcacao_df["Date"] == data_inteira)]
        if ne_pld_publicado['Value'].values[0] != 0:
            ne = ne_pld_publicado
        else:
            ne = tabMarcacao_df[
                (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD Implicito NE") & (
                        tabMarcacao_df["Date"] == data_inteira)]
        if n_pld_publicado['Value'].values[0] != 0:
            n = n_pld_publicado
        else:
            n = tabMarcacao_df[
                (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD Implicito NORTE") & (
                        tabMarcacao_df["Date"] == data_inteira)]

        # Fio
        i0 = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP I0") & (
                    tabMarcacao_df["Date"] == data_inteira)]
        i5 = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP I5") & (
                    tabMarcacao_df["Date"] == data_inteira)]
        i8 = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP I8") & (
                    tabMarcacao_df["Date"] == data_inteira)]
        i1 = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP I1") & (
                    tabMarcacao_df["Date"] == data_inteira)]
        cq5 = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP CQ5") & (
                    tabMarcacao_df["Date"] == data_inteira)]
        ly = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP Iy") & (
                    tabMarcacao_df["Date"] == data_inteira)]
        # Agio
        agio_se = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD+ SE/CO") & (
                    tabMarcacao_df["Date"] == data_inteira)]
        agio_s = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD+ SUL") & (
                    tabMarcacao_df["Date"] == data_inteira)]
        agio_ne = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD+ NE") & (
                    tabMarcacao_df["Date"] == data_inteira)]
        agio_n = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD+ NORTE") & (
                    tabMarcacao_df["Date"] == data_inteira)]

        # inflação
        if int(data_split[1]) < 10:
            aux = data_split[1]
            aux = aux.replace("0", "")
            inflacao = df_inflacao_cenario[
                (df_inflacao_cenario["CENARIO"] == "Base") & (df_inflacao_cenario["MES"] == int(aux)) & (
                        df_inflacao_cenario["ANO"] == int(data_split[0]))]
        else:
            inflacao = df_inflacao_cenario[
                (df_inflacao_cenario["CENARIO"] == "Base") & (df_inflacao_cenario["MES"] == int(data_split[1])) & (
                        df_inflacao_cenario["ANO"] == int(data_split[0]))]

        # Fixo (PLD + Swap) (Regra no caderno: Se mes < data_ccee return PLD-Publicado,
        # senão se mes = data_ccee, se pld_publicado <> 0 então return pld_publicado + fio + agio,
        # senão se mes > data_ccee, então return pld_implicito + fio + agio
        # SE
        fixo_se_i0 = se['Value'].values[0] + i0['Value'].values[0] + agio_se['Value'].values[0]
        fixo_se_i5 = se['Value'].values[0] + i5['Value'].values[0] + agio_se['Value'].values[0]
        fixo_se_i8 = se['Value'].values[0] + i8['Value'].values[0] + agio_se['Value'].values[0]
        fixo_se_i1 = se['Value'].values[0] + i1['Value'].values[0] + agio_se['Value'].values[0]
        fixo_se_cq5 = se['Value'].values[0] + cq5['Value'].values[0] + agio_se['Value'].values[0]
        fixo_se_ly = se['Value'].values[0] + ly['Value'].values[0] + agio_se['Value'].values[0]
        # S
        fixo_s_i0 = s['Value'].values[0] + i0['Value'].values[0] + agio_s['Value'].values[0]
        fixo_s_i5 = s['Value'].values[0] + i5['Value'].values[0] + agio_s['Value'].values[0]
        fixo_s_i8 = s['Value'].values[0] + i8['Value'].values[0] + agio_s['Value'].values[0]
        fixo_s_i1 = s['Value'].values[0] + i1['Value'].values[0] + agio_s['Value'].values[0]
        fixo_s_cq5 = s['Value'].values[0] + cq5['Value'].values[0] + agio_s['Value'].values[0]
        fixo_s_ly = s['Value'].values[0] + ly['Value'].values[0] + agio_s['Value'].values[0]
        # NE
        fixo_ne_i0 = ne['Value'].values[0] + i0['Value'].values[0] + agio_ne['Value'].values[0]
        fixo_ne_i5 = ne['Value'].values[0] + i5['Value'].values[0] + agio_ne['Value'].values[0]
        fixo_ne_i8 = ne['Value'].values[0] + i8['Value'].values[0] + agio_ne['Value'].values[0]
        fixo_ne_i1 = ne['Value'].values[0] + i1['Value'].values[0] + agio_ne['Value'].values[0]
        fixo_ne_cq5 = ne['Value'].values[0] + cq5['Value'].values[0] + agio_ne['Value'].values[0]
        fixo_ne_ly = ne['Value'].values[0] + ly['Value'].values[0] + agio_ne['Value'].values[0]
        # N
        fixo_n_i0 = n['Value'].values[0] + i0['Value'].values[0] + agio_n['Value'].values[0]
        fixo_n_i5 = n['Value'].values[0] + i5['Value'].values[0] + agio_n['Value'].values[0]
        fixo_n_i8 = n['Value'].values[0] + i8['Value'].values[0] + agio_n['Value'].values[0]
        fixo_n_i1 = n['Value'].values[0] + i1['Value'].values[0] + agio_n['Value'].values[0]
        fixo_n_cq5 = n['Value'].values[0] + cq5['Value'].values[0] + agio_n['Value'].values[0]
        fixo_n_ly = n['Value'].values[0] + ly['Value'].values[0] + agio_n['Value'].values[0]
        # ['Cenario', 'Ano', 'Mes', 'Submercado', 'Energia', 'PLD', 'Fixo', 'Fixo_infl', 'Pos', 'Pos_infl']

        data_inteira = datetime.datetime.strptime(data_inteira, '%Y-%m-%d')
        data_ccee = data_ref_ccee['valor'].values[0]
        # test = datetime.datetime.strptime(str(from_date), '%Y-%m-%d').date()"%Y-%m-%dT%H:%M:%S"
        data_ccee = datetime.datetime.strptime(str(data_ccee), '%Y-%m-%dT%H:%M:%S.%f000')

        if data_inteira < data_ccee:
            #         #     retornar PLD_Publicado SE/CO
            row.append(
                ['BASE', data_split[0], data_split[1], 'SE/CO', 'Convencional', se_pld_publicado['Value'].values[0],
                 se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0],
                 se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-0%', se_pld_publicado['Value'].values[0],
                 se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0],
                 se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-50%', se_pld_publicado['Value'].values[0],
                 se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0],
                 se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-80%', se_pld_publicado['Value'].values[0],
                 se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0],
                 se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-100%', se_pld_publicado['Value'].values[0],
                 se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0],
                 se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0]])
            row.append(['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-CQ50%',
                        se_pld_publicado['Value'].values[0],
                        se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0],
                        se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-y', se_pld_publicado['Value'].values[0],
                 se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0],
                 se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0]])
            #     retornar PLD_Publicado SUL
            row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Convencional', s_pld_publicado['Value'].values[0],
                        s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0],
                        s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0]])
            row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-0%', s_pld_publicado['Value'].values[0],
                        s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0],
                        s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-50%', s_pld_publicado['Value'].values[0],
                 s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0],
                 s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-80%', s_pld_publicado['Value'].values[0],
                 s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0],
                 s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-100%', s_pld_publicado['Value'].values[0],
                 s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0],
                 s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-CQ50%', s_pld_publicado['Value'].values[0],
                 s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0],
                 s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0]])
            row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-y', s_pld_publicado['Value'].values[0],
                        s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0],
                        s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0]])
            #     retornar PLD_Publicado NE
            row.append(['BASE', data_split[0], data_split[1], 'NE', 'Convencional', ne_pld_publicado['Value'].values[0],
                        ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0],
                        ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-0%', ne_pld_publicado['Value'].values[0],
                 ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0],
                 ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-50%', ne_pld_publicado['Value'].values[0],
                 ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0],
                 ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-80%', ne_pld_publicado['Value'].values[0],
                 ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0],
                 ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-100%', ne_pld_publicado['Value'].values[0],
                 ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0],
                 ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-CQ50%', ne_pld_publicado['Value'].values[0],
                 ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0],
                 ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-y', ne_pld_publicado['Value'].values[0],
                 ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0],
                 ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0]])
            #     retornar PLD_Publicado N
            row.append(
                ['BASE', data_split[0], data_split[1], 'NORTE', 'Convencional', n_pld_publicado['Value'].values[0],
                 n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0],
                 n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-0%', n_pld_publicado['Value'].values[0],
                 n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0],
                 n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-50%', n_pld_publicado['Value'].values[0],
                 n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0],
                 n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-80%', n_pld_publicado['Value'].values[0],
                 n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0],
                 n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-100%', n_pld_publicado['Value'].values[0],
                 n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0],
                 n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-CQ50%', n_pld_publicado['Value'].values[0],
                 n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0],
                 n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-y', n_pld_publicado['Value'].values[0],
                 n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0],
                 n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0]])

        if data_inteira >= data_ccee:
            #     retornar se +fio +agio
            row.append(['BASE', data_split[0], data_split[1], 'SE/CO', 'Convencional', se['Value'].values[0],
                        se['Value'].values[0], inflacao['CORRECAO'].values[0] * se['Value'].values[0],
                        se['Value'].values[0], inflacao['CORRECAO'].values[0] * se['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-0%', se['Value'].values[0], fixo_se_i0,
                 inflacao['CORRECAO'].values[0] * fixo_se_i0, fixo_se_i0, inflacao['CORRECAO'].values[0] * fixo_se_i0])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-50%', se['Value'].values[0], fixo_se_i5,
                 inflacao['CORRECAO'].values[0] * fixo_se_i5, fixo_se_i5, inflacao['CORRECAO'].values[0] * fixo_se_i5])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-80%', se['Value'].values[0], fixo_se_i8,
                 inflacao['CORRECAO'].values[0] * fixo_se_i8, fixo_se_i8, inflacao['CORRECAO'].values[0] * fixo_se_i8])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-100%', se['Value'].values[0], fixo_se_i1,
                 inflacao['CORRECAO'].values[0] * fixo_se_i1, fixo_se_i1, inflacao['CORRECAO'].values[0] * fixo_se_i1])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-CQ50%', se['Value'].values[0], fixo_se_cq5,
                 inflacao['CORRECAO'].values[0] * fixo_se_cq5, fixo_se_cq5,
                 inflacao['CORRECAO'].values[0] * fixo_se_cq5])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-y', se['Value'].values[0], fixo_se_ly,
                 inflacao['CORRECAO'].values[0] * fixo_se_ly, fixo_se_ly, inflacao['CORRECAO'].values[0] * fixo_se_ly])
            #     retornar s +fio +agio
            row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Convencional', s['Value'].values[0],
                        s['Value'].values[0], inflacao['CORRECAO'].values[0] * s['Value'].values[0],
                        s['Value'].values[0], inflacao['CORRECAO'].values[0] * s['Value'].values[0]])
            row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-0%', s['Value'].values[0], fixo_s_i0,
                        inflacao['CORRECAO'].values[0] * fixo_s_i0, fixo_s_i0,
                        inflacao['CORRECAO'].values[0] * fixo_s_i0])
            row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-50%', s['Value'].values[0], fixo_s_i5,
                        inflacao['CORRECAO'].values[0] * fixo_s_i5, fixo_s_i5,
                        inflacao['CORRECAO'].values[0] * fixo_s_i5])
            row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-80%', s['Value'].values[0], fixo_s_i8,
                        inflacao['CORRECAO'].values[0] * fixo_s_i8, fixo_s_i8,
                        inflacao['CORRECAO'].values[0] * fixo_s_i8])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-100%', s['Value'].values[0], fixo_s_i1,
                 inflacao['CORRECAO'].values[0] * fixo_s_i1, fixo_s_i1, inflacao['CORRECAO'].values[0] * fixo_s_i1])
            row.append(
                ['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-CQ50%', s['Value'].values[0], fixo_s_cq5,
                 inflacao['CORRECAO'].values[0] * fixo_s_cq5, fixo_s_cq5, inflacao['CORRECAO'].values[0] * fixo_s_cq5])
            row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-y', s['Value'].values[0], fixo_s_ly,
                        inflacao['CORRECAO'].values[0] * fixo_s_ly, fixo_s_ly,
                        inflacao['CORRECAO'].values[0] * fixo_s_ly])
            #     retornar ne +fio +agio

            row.append(['BASE', data_split[0], data_split[1], 'NE', 'Convencional', ne['Value'].values[0],
                        ne['Value'].values[0], inflacao['CORRECAO'].values[0] * ne['Value'].values[0],
                        ne['Value'].values[0], inflacao['CORRECAO'].values[0] * ne['Value'].values[0]])
            row.append(['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-0%', ne['Value'].values[0], fixo_ne_i0,
                        inflacao['CORRECAO'].values[0] * fixo_ne_i0, fixo_ne_i0,
                        inflacao['CORRECAO'].values[0] * fixo_ne_i0])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-50%', ne['Value'].values[0], fixo_ne_i5,
                 inflacao['CORRECAO'].values[0] * fixo_ne_i5, fixo_ne_i5, inflacao['CORRECAO'].values[0] * fixo_ne_i5])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-80%', ne['Value'].values[0], fixo_ne_i8,
                 inflacao['CORRECAO'].values[0] * fixo_ne_i8, fixo_ne_i8, inflacao['CORRECAO'].values[0] * fixo_ne_i8])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-100%', ne['Value'].values[0], fixo_ne_i1,
                 inflacao['CORRECAO'].values[0] * fixo_ne_i1, fixo_ne_i1, inflacao['CORRECAO'].values[0] * fixo_ne_i1])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-CQ50%', ne['Value'].values[0], fixo_ne_cq5,
                 inflacao['CORRECAO'].values[0] * fixo_ne_cq5, fixo_ne_cq5,
                 inflacao['CORRECAO'].values[0] * fixo_ne_cq5])
            row.append(['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-y', ne['Value'].values[0], fixo_ne_ly,
                        inflacao['CORRECAO'].values[0] * fixo_ne_ly, fixo_ne_ly,
                        inflacao['CORRECAO'].values[0] * fixo_ne_ly])
            #     retornar n +fio +agio
            row.append(['BASE', data_split[0], data_split[1], 'NORTE', 'Convencional', n['Value'].values[0],
                        n['Value'].values[0], inflacao['CORRECAO'].values[0] * n['Value'].values[0],
                        n['Value'].values[0], inflacao['CORRECAO'].values[0] * n['Value'].values[0]])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-0%', n['Value'].values[0], fixo_n_i0,
                 inflacao['CORRECAO'].values[0] * fixo_n_i0, fixo_n_i0, inflacao['CORRECAO'].values[0] * fixo_n_i0])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-50%', n['Value'].values[0], fixo_n_i5,
                 inflacao['CORRECAO'].values[0] * fixo_n_i5, fixo_n_i5, inflacao['CORRECAO'].values[0] * fixo_n_i5])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-80%', n['Value'].values[0], fixo_n_i8,
                 inflacao['CORRECAO'].values[0] * fixo_n_i8, fixo_n_i8, inflacao['CORRECAO'].values[0] * fixo_n_i8])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-100%', n['Value'].values[0], fixo_n_i1,
                 inflacao['CORRECAO'].values[0] * fixo_n_i1, fixo_n_i1, inflacao['CORRECAO'].values[0] * fixo_n_i1])
            row.append(
                ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-CQ50%', n['Value'].values[0], fixo_n_cq5,
                 inflacao['CORRECAO'].values[0] * fixo_n_cq5, fixo_n_cq5, inflacao['CORRECAO'].values[0] * fixo_n_cq5])
            row.append(['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-y', n['Value'].values[0], fixo_n_ly,
                        inflacao['CORRECAO'].values[0] * fixo_n_ly, fixo_n_ly,
                        inflacao['CORRECAO'].values[0] * fixo_n_ly])

    df_tabmkt = pd.DataFrame(row, columns=['Cenario', 'Ano', 'Mes', 'Submercado', 'Energia', 'PLD', 'Fixo', 'Fixo_infl',
                                           'Pos', 'Pos_infl'])
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_mkt_mensal', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)

    print('tab_mkt_mensal - exec_time  = {} seconds'.format(exec_time.seconds))
    return df_tabmkt


def tab_mkt_anual(tabMarcacao_df, df_inflacao_cenario, data_ini, data_fim):
    init_time = datetime.datetime.now()
    row = []
    dates = pd.date_range(data_ini, data_fim, freq='MS').strftime("%Y/%m").tolist()
    from sqlalchemy import create_engine
    engine = create_engine(credenciais)
    data_ref_ccee = pd.read_sql(sql='Select valor from parametros where parametro = "ref_ccee" ', con=engine)
    for data in dates:
        data_split = data.split('/')
        data_inteira = data + "/01"
        data_inteira = data_inteira.replace('/', '-')

        # PLD ( Pegar PLD publicado quando houver, senão pegar o PLD Implicito

        #  PLD Publicado
        se_pld_publicado = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD SE/CO") & (
                    tabMarcacao_df["Date"] == data_split[0])]
        s_pld_publicado = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD SUL") & (
                    tabMarcacao_df["Date"] == data_split[0])]
        ne_pld_publicado = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD NE") & (
                    tabMarcacao_df["Date"] == data_split[0])]
        n_pld_publicado = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD NORTE") & (
                    tabMarcacao_df["Date"] == data_split[0])]

        # PLD

        se = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD Implicito SE/CO") & (
                    tabMarcacao_df["Date"] == data_split[0])]

        s = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD Implicito SUL") & (
                    tabMarcacao_df["Date"] == data_split[0])]

        ne = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD Implicito NE") & (
                    tabMarcacao_df["Date"] == data_split[0])]

        n = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD Implicito NORTE") & (
                    tabMarcacao_df["Date"] == data_split[0])]

        # Fio
        i0 = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP I0") & (
                    tabMarcacao_df["Date"] == data_split[0])]
        i5 = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP I5") & (
                    tabMarcacao_df["Date"] == data_split[0])]
        i8 = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP I8") & (
                    tabMarcacao_df["Date"] == data_split[0])]
        i1 = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP I1") & (
                    tabMarcacao_df["Date"] == data_split[0])]
        cq5 = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP CQ5") & (
                    tabMarcacao_df["Date"] == data_split[0])]
        ly = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP Iy") & (
                    tabMarcacao_df["Date"] == data_split[0])]
        # Agio
        agio_se = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD+ SE/CO") & (
                    tabMarcacao_df["Date"] == data_split[0])]
        agio_s = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD+ SUL") & (
                    tabMarcacao_df["Date"] == data_split[0])]
        agio_ne = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD+ NE") & (
                    tabMarcacao_df["Date"] == data_split[0])]
        agio_n = tabMarcacao_df[
            (tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD+ NORTE") & (
                    tabMarcacao_df["Date"] == data_split[0])]

        # inflação
        if int(data_split[1]) < 10:
            aux = data_split[1]
            aux = aux.replace("0", "")
            inflacao = df_inflacao_cenario[
                (df_inflacao_cenario["CENARIO"] == "Base") & (df_inflacao_cenario["MES"] == int(aux)) & (
                        df_inflacao_cenario["ANO"] == int(data_split[0]))]
        else:
            inflacao = df_inflacao_cenario[
                (df_inflacao_cenario["CENARIO"] == "Base") & (df_inflacao_cenario["MES"] == int(data_split[1])) & (
                        df_inflacao_cenario["ANO"] == int(data_split[0]))]

        # Fixo (PLD + Swap) (Regra no caderno: Se mes < data_ccee return PLD-Publicado,
        # senão se mes = data_ccee, se pld_publicado <> 0 então return pld_publicado + fio + agio,
        # senão se mes > data_ccee, então return pld_implicito + fio + agio
        # SE
        fixo_se_i0 = se['Value'].values[0] + i0['Value'].values[0] + agio_se['Value'].values[0]
        fixo_se_i5 = se['Value'].values[0] + i5['Value'].values[0] + agio_se['Value'].values[0]
        fixo_se_i8 = se['Value'].values[0] + i8['Value'].values[0] + agio_se['Value'].values[0]
        fixo_se_i1 = se['Value'].values[0] + i1['Value'].values[0] + agio_se['Value'].values[0]
        fixo_se_cq5 = se['Value'].values[0] + cq5['Value'].values[0] + agio_se['Value'].values[0]
        fixo_se_ly = se['Value'].values[0] + ly['Value'].values[0] + agio_se['Value'].values[0]
        # S
        fixo_s_i0 = s['Value'].values[0] + i0['Value'].values[0] + agio_s['Value'].values[0]
        fixo_s_i5 = s['Value'].values[0] + i5['Value'].values[0] + agio_s['Value'].values[0]
        fixo_s_i8 = s['Value'].values[0] + i8['Value'].values[0] + agio_s['Value'].values[0]
        fixo_s_i1 = s['Value'].values[0] + i1['Value'].values[0] + agio_s['Value'].values[0]
        fixo_s_cq5 = s['Value'].values[0] + cq5['Value'].values[0] + agio_s['Value'].values[0]
        fixo_s_ly = s['Value'].values[0] + ly['Value'].values[0] + agio_s['Value'].values[0]
        # NE
        fixo_ne_i0 = ne['Value'].values[0] + i0['Value'].values[0] + agio_ne['Value'].values[0]
        fixo_ne_i5 = ne['Value'].values[0] + i5['Value'].values[0] + agio_ne['Value'].values[0]
        fixo_ne_i8 = ne['Value'].values[0] + i8['Value'].values[0] + agio_ne['Value'].values[0]
        fixo_ne_i1 = ne['Value'].values[0] + i1['Value'].values[0] + agio_ne['Value'].values[0]
        fixo_ne_cq5 = ne['Value'].values[0] + cq5['Value'].values[0] + agio_ne['Value'].values[0]
        fixo_ne_ly = ne['Value'].values[0] + ly['Value'].values[0] + agio_ne['Value'].values[0]
        # N
        fixo_n_i0 = n['Value'].values[0] + i0['Value'].values[0] + agio_n['Value'].values[0]
        fixo_n_i5 = n['Value'].values[0] + i5['Value'].values[0] + agio_n['Value'].values[0]
        fixo_n_i8 = n['Value'].values[0] + i8['Value'].values[0] + agio_n['Value'].values[0]
        fixo_n_i1 = n['Value'].values[0] + i1['Value'].values[0] + agio_n['Value'].values[0]
        fixo_n_cq5 = n['Value'].values[0] + cq5['Value'].values[0] + agio_n['Value'].values[0]
        fixo_n_ly = n['Value'].values[0] + ly['Value'].values[0] + agio_n['Value'].values[0]

        # ['Cenario', 'Ano', 'Mes', 'Submercado', 'Energia', 'PLD', 'Fixo', 'Fixo_infl', 'Pos', 'Pos_infl']

        data_inteira = datetime.datetime.strptime(data_inteira, '%Y-%m-%d')
        data_ccee = data_ref_ccee['valor'].values[0]
        # test = datetime.datetime.strptime(str(from_date), '%Y-%m-%d').date()"%Y-%m-%dT%H:%M:%S"
        data_ccee = datetime.datetime.strptime(str(data_ccee), '%Y-%m-%dT%H:%M:%S.%f000')

        # if data_inteira < data_ccee:
            #         #     retornar PLD_Publicado SE/CO
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'SE/CO', 'Convencional', se_pld_publicado['Value'].values[0],
        #          se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0],
        #          se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-0%', se_pld_publicado['Value'].values[0],
        #          se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0],
        #          se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-50%', se_pld_publicado['Value'].values[0],
        #          se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0],
        #          se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-80%', se_pld_publicado['Value'].values[0],
        #          se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0],
        #          se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-100%', se_pld_publicado['Value'].values[0],
        #          se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0],
        #          se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0]])
        #     row.append(['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-CQ50%',
        #                 se_pld_publicado['Value'].values[0],
        #                 se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0],
        #                 se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-y', se_pld_publicado['Value'].values[0],
        #          se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0],
        #          se_pld_publicado['Value'].values[0], se_pld_publicado['Value'].values[0]])
        #     #     retornar PLD_Publicado SUL
        #     row.append(['BASE', data_split[0], data_split[1], 'S', 'Convencional', s_pld_publicado['Value'].values[0],
        #                 s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0],
        #                 s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0]])
        #     row.append(['BASE', data_split[0], data_split[1], 'S', 'Incentivada-0%', s_pld_publicado['Value'].values[0],
        #                 s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0],
        #                 s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'S', 'Incentivada-50%', s_pld_publicado['Value'].values[0],
        #          s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0],
        #          s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'S', 'Incentivada-80%', s_pld_publicado['Value'].values[0],
        #          s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0],
        #          s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'S', 'Incentivada-100%', s_pld_publicado['Value'].values[0],
        #          s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0],
        #          s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'S', 'Incentivada-CQ50%', s_pld_publicado['Value'].values[0],
        #          s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0],
        #          s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0]])
        #     row.append(['BASE', data_split[0], data_split[1], 'S', 'Incentivada-y', s_pld_publicado['Value'].values[0],
        #                 s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0],
        #                 s_pld_publicado['Value'].values[0], s_pld_publicado['Value'].values[0]])
        #     #     retornar PLD_Publicado NE
        #     row.append(['BASE', data_split[0], data_split[1], 'NE', 'Convencional', ne_pld_publicado['Value'].values[0],
        #                 ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0],
        #                 ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-0%', ne_pld_publicado['Value'].values[0],
        #          ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0],
        #          ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-50%', ne_pld_publicado['Value'].values[0],
        #          ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0],
        #          ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-80%', ne_pld_publicado['Value'].values[0],
        #          ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0],
        #          ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-100%', ne_pld_publicado['Value'].values[0],
        #          ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0],
        #          ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-CQ50%', ne_pld_publicado['Value'].values[0],
        #          ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0],
        #          ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-y', ne_pld_publicado['Value'].values[0],
        #          ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0],
        #          ne_pld_publicado['Value'].values[0], ne_pld_publicado['Value'].values[0]])
        #     #     retornar PLD_Publicado N
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NORTE', 'Convencional', n_pld_publicado['Value'].values[0],
        #          n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0],
        #          n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-0%', n_pld_publicado['Value'].values[0],
        #          n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0],
        #          n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-50%', n_pld_publicado['Value'].values[0],
        #          n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0],
        #          n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-80%', n_pld_publicado['Value'].values[0],
        #          n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0],
        #          n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-100%', n_pld_publicado['Value'].values[0],
        #          n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0],
        #          n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-CQ50%', n_pld_publicado['Value'].values[0],
        #          n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0],
        #          n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-y', n_pld_publicado['Value'].values[0],
        #          n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0],
        #          n_pld_publicado['Value'].values[0], n_pld_publicado['Value'].values[0]])
        #
        # if data_inteira >= data_ccee:
        #     #     retornar se +fio +agio
        #     row.append(['BASE', data_split[0], data_split[1], 'SE/CO', 'Convencional', se['Value'].values[0],
        #                 se['Value'].values[0], inflacao['CORRECAO'].values[0] * se['Value'].values[0],
        #                 se['Value'].values[0], inflacao['CORRECAO'].values[0] * se['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-0%', se['Value'].values[0], fixo_se_i0,
        #          inflacao['CORRECAO'].values[0] * fixo_se_i0, fixo_se_i0, inflacao['CORRECAO'].values[0] * fixo_se_i0])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-50%', se['Value'].values[0], fixo_se_i5,
        #          inflacao['CORRECAO'].values[0] * fixo_se_i5, fixo_se_i5, inflacao['CORRECAO'].values[0] * fixo_se_i5])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-80%', se['Value'].values[0], fixo_se_i8,
        #          inflacao['CORRECAO'].values[0] * fixo_se_i8, fixo_se_i8, inflacao['CORRECAO'].values[0] * fixo_se_i8])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-100%', se['Value'].values[0], fixo_se_i1,
        #          inflacao['CORRECAO'].values[0] * fixo_se_i1, fixo_se_i1, inflacao['CORRECAO'].values[0] * fixo_se_i1])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-CQ50%', se['Value'].values[0], fixo_se_cq5,
        #          inflacao['CORRECAO'].values[0] * fixo_se_cq5, fixo_se_cq5,
        #          inflacao['CORRECAO'].values[0] * fixo_se_cq5])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-y', se['Value'].values[0], fixo_se_ly,
        #          inflacao['CORRECAO'].values[0] * fixo_se_ly, fixo_se_ly, inflacao['CORRECAO'].values[0] * fixo_se_ly])
        #     #     retornar s +fio +agio
        #     row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Convencional', s['Value'].values[0],
        #                 s['Value'].values[0], inflacao['CORRECAO'].values[0] * s['Value'].values[0],
        #                 s['Value'].values[0], inflacao['CORRECAO'].values[0] * s['Value'].values[0]])
        #     row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-0%', s['Value'].values[0], fixo_s_i0,
        #                 inflacao['CORRECAO'].values[0] * fixo_s_i0, fixo_s_i0,
        #                 inflacao['CORRECAO'].values[0] * fixo_s_i0])
        #     row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-50%', s['Value'].values[0], fixo_s_i5,
        #                 inflacao['CORRECAO'].values[0] * fixo_s_i5, fixo_s_i5,
        #                 inflacao['CORRECAO'].values[0] * fixo_s_i5])
        #     row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-80%', s['Value'].values[0], fixo_s_i8,
        #                 inflacao['CORRECAO'].values[0] * fixo_s_i8, fixo_s_i8,
        #                 inflacao['CORRECAO'].values[0] * fixo_s_i8])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-100%', s['Value'].values[0], fixo_s_i1,
        #          inflacao['CORRECAO'].values[0] * fixo_s_i1, fixo_s_i1, inflacao['CORRECAO'].values[0] * fixo_s_i1])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-CQ50%', s['Value'].values[0], fixo_s_cq5,
        #          inflacao['CORRECAO'].values[0] * fixo_s_cq5, fixo_s_cq5, inflacao['CORRECAO'].values[0] * fixo_s_cq5])
        #     row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-y', s['Value'].values[0], fixo_s_ly,
        #                 inflacao['CORRECAO'].values[0] * fixo_s_ly, fixo_s_ly,
        #                 inflacao['CORRECAO'].values[0] * fixo_s_ly])
        #     #     retornar ne +fio +agio
        #
        #     row.append(['BASE', data_split[0], data_split[1], 'NE', 'Convencional', ne['Value'].values[0],
        #                 ne['Value'].values[0], inflacao['CORRECAO'].values[0] * ne['Value'].values[0],
        #                 ne['Value'].values[0], inflacao['CORRECAO'].values[0] * ne['Value'].values[0]])
        #     row.append(['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-0%', ne['Value'].values[0], fixo_ne_i0,
        #                 inflacao['CORRECAO'].values[0] * fixo_ne_i0, fixo_ne_i0,
        #                 inflacao['CORRECAO'].values[0] * fixo_ne_i0])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-50%', ne['Value'].values[0], fixo_ne_i5,
        #          inflacao['CORRECAO'].values[0] * fixo_ne_i5, fixo_ne_i5, inflacao['CORRECAO'].values[0] * fixo_ne_i5])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-80%', ne['Value'].values[0], fixo_ne_i8,
        #          inflacao['CORRECAO'].values[0] * fixo_ne_i8, fixo_ne_i8, inflacao['CORRECAO'].values[0] * fixo_ne_i8])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-100%', ne['Value'].values[0], fixo_ne_i1,
        #          inflacao['CORRECAO'].values[0] * fixo_ne_i1, fixo_ne_i1, inflacao['CORRECAO'].values[0] * fixo_ne_i1])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-CQ50%', ne['Value'].values[0], fixo_ne_cq5,
        #          inflacao['CORRECAO'].values[0] * fixo_ne_cq5, fixo_ne_cq5,
        #          inflacao['CORRECAO'].values[0] * fixo_ne_cq5])
        #     row.append(['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-y', ne['Value'].values[0], fixo_ne_ly,
        #                 inflacao['CORRECAO'].values[0] * fixo_ne_ly, fixo_ne_ly,
        #                 inflacao['CORRECAO'].values[0] * fixo_ne_ly])
        #     #     retornar n +fio +agio
        #     row.append(['BASE', data_split[0], data_split[1], 'NORTE', 'Convencional', n['Value'].values[0],
        #                 n['Value'].values[0], inflacao['CORRECAO'].values[0] * n['Value'].values[0],
        #                 n['Value'].values[0], inflacao['CORRECAO'].values[0] * n['Value'].values[0]])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-0%', n['Value'].values[0], fixo_n_i0,
        #          inflacao['CORRECAO'].values[0] * fixo_n_i0, fixo_n_i0, inflacao['CORRECAO'].values[0] * fixo_n_i0])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-50%', n['Value'].values[0], fixo_n_i5,
        #          inflacao['CORRECAO'].values[0] * fixo_n_i5, fixo_n_i5, inflacao['CORRECAO'].values[0] * fixo_n_i5])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-80%', n['Value'].values[0], fixo_n_i8,
        #          inflacao['CORRECAO'].values[0] * fixo_n_i8, fixo_n_i8, inflacao['CORRECAO'].values[0] * fixo_n_i8])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-100%', n['Value'].values[0], fixo_n_i1,
        #          inflacao['CORRECAO'].values[0] * fixo_n_i1, fixo_n_i1, inflacao['CORRECAO'].values[0] * fixo_n_i1])
        #     row.append(
        #         ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-CQ50%', n['Value'].values[0], fixo_n_cq5,
        #          inflacao['CORRECAO'].values[0] * fixo_n_cq5, fixo_n_cq5, inflacao['CORRECAO'].values[0] * fixo_n_cq5])
        #     row.append(['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-y', n['Value'].values[0], fixo_n_ly,
        #                 inflacao['CORRECAO'].values[0] * fixo_n_ly, fixo_n_ly,
        #                 inflacao['CORRECAO'].values[0] * fixo_n_ly])

        row.append(['BASE', data_split[0], data_split[1], 'SE/CO', 'Convencional', se['Value'].values[0],
                    se['Value'].values[0], inflacao['CORRECAO'].values[0] * se['Value'].values[0],
                    se['Value'].values[0], inflacao['CORRECAO'].values[0] * se['Value'].values[0]])
        row.append(
            ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-0%', se['Value'].values[0], fixo_se_i0,
             inflacao['CORRECAO'].values[0] * fixo_se_i0, fixo_se_i0, inflacao['CORRECAO'].values[0] * fixo_se_i0])
        row.append(
            ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-50%', se['Value'].values[0], fixo_se_i5,
             inflacao['CORRECAO'].values[0] * fixo_se_i5, fixo_se_i5, inflacao['CORRECAO'].values[0] * fixo_se_i5])
        row.append(
            ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-80%', se['Value'].values[0], fixo_se_i8,
             inflacao['CORRECAO'].values[0] * fixo_se_i8, fixo_se_i8, inflacao['CORRECAO'].values[0] * fixo_se_i8])
        row.append(
            ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-100%', se['Value'].values[0], fixo_se_i1,
             inflacao['CORRECAO'].values[0] * fixo_se_i1, fixo_se_i1, inflacao['CORRECAO'].values[0] * fixo_se_i1])
        row.append(
            ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-CQ50%', se['Value'].values[0], fixo_se_cq5,
             inflacao['CORRECAO'].values[0] * fixo_se_cq5, fixo_se_cq5,
             inflacao['CORRECAO'].values[0] * fixo_se_cq5])
        row.append(
            ['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-y', se['Value'].values[0], fixo_se_ly,
             inflacao['CORRECAO'].values[0] * fixo_se_ly, fixo_se_ly, inflacao['CORRECAO'].values[0] * fixo_se_ly])

        row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Convencional', s['Value'].values[0],
                    s['Value'].values[0], inflacao['CORRECAO'].values[0] * s['Value'].values[0],
                    s['Value'].values[0], inflacao['CORRECAO'].values[0] * s['Value'].values[0]])
        row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-0%', s['Value'].values[0], fixo_s_i0,
                    inflacao['CORRECAO'].values[0] * fixo_s_i0, fixo_s_i0,
                    inflacao['CORRECAO'].values[0] * fixo_s_i0])
        row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-50%', s['Value'].values[0], fixo_s_i5,
                    inflacao['CORRECAO'].values[0] * fixo_s_i5, fixo_s_i5,
                    inflacao['CORRECAO'].values[0] * fixo_s_i5])
        row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-80%', s['Value'].values[0], fixo_s_i8,
                    inflacao['CORRECAO'].values[0] * fixo_s_i8, fixo_s_i8,
                    inflacao['CORRECAO'].values[0] * fixo_s_i8])
        row.append(
            ['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-100%', s['Value'].values[0], fixo_s_i1,
             inflacao['CORRECAO'].values[0] * fixo_s_i1, fixo_s_i1, inflacao['CORRECAO'].values[0] * fixo_s_i1])
        row.append(
            ['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-CQ50%', s['Value'].values[0], fixo_s_cq5,
             inflacao['CORRECAO'].values[0] * fixo_s_cq5, fixo_s_cq5, inflacao['CORRECAO'].values[0] * fixo_s_cq5])
        row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-y', s['Value'].values[0], fixo_s_ly,
                    inflacao['CORRECAO'].values[0] * fixo_s_ly, fixo_s_ly,
                    inflacao['CORRECAO'].values[0] * fixo_s_ly])

        row.append(['BASE', data_split[0], data_split[1], 'NE', 'Convencional', ne['Value'].values[0],
                    ne['Value'].values[0], inflacao['CORRECAO'].values[0] * ne['Value'].values[0],
                    ne['Value'].values[0], inflacao['CORRECAO'].values[0] * ne['Value'].values[0]])
        row.append(['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-0%', ne['Value'].values[0], fixo_ne_i0,
                    inflacao['CORRECAO'].values[0] * fixo_ne_i0, fixo_ne_i0,
                    inflacao['CORRECAO'].values[0] * fixo_ne_i0])
        row.append(
            ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-50%', ne['Value'].values[0], fixo_ne_i5,
             inflacao['CORRECAO'].values[0] * fixo_ne_i5, fixo_ne_i5, inflacao['CORRECAO'].values[0] * fixo_ne_i5])
        row.append(
            ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-80%', ne['Value'].values[0], fixo_ne_i8,
             inflacao['CORRECAO'].values[0] * fixo_ne_i8, fixo_ne_i8, inflacao['CORRECAO'].values[0] * fixo_ne_i8])
        row.append(
            ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-100%', ne['Value'].values[0], fixo_ne_i1,
             inflacao['CORRECAO'].values[0] * fixo_ne_i1, fixo_ne_i1, inflacao['CORRECAO'].values[0] * fixo_ne_i1])
        row.append(
            ['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-CQ50%', ne['Value'].values[0], fixo_ne_cq5,
             inflacao['CORRECAO'].values[0] * fixo_ne_cq5, fixo_ne_cq5,
             inflacao['CORRECAO'].values[0] * fixo_ne_cq5])
        row.append(['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-y', ne['Value'].values[0], fixo_ne_ly,
                    inflacao['CORRECAO'].values[0] * fixo_ne_ly, fixo_ne_ly,
                    inflacao['CORRECAO'].values[0] * fixo_ne_ly])

        row.append(['BASE', data_split[0], data_split[1], 'NORTE', 'Convencional', n['Value'].values[0],
                    n['Value'].values[0], inflacao['CORRECAO'].values[0] * n['Value'].values[0],
                    n['Value'].values[0], inflacao['CORRECAO'].values[0] * n['Value'].values[0]])
        row.append(
            ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-0%', n['Value'].values[0], fixo_n_i0,
             inflacao['CORRECAO'].values[0] * fixo_n_i0, fixo_n_i0, inflacao['CORRECAO'].values[0] * fixo_n_i0])
        row.append(
            ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-50%', n['Value'].values[0], fixo_n_i5,
             inflacao['CORRECAO'].values[0] * fixo_n_i5, fixo_n_i5, inflacao['CORRECAO'].values[0] * fixo_n_i5])
        row.append(
            ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-80%', n['Value'].values[0], fixo_n_i8,
             inflacao['CORRECAO'].values[0] * fixo_n_i8, fixo_n_i8, inflacao['CORRECAO'].values[0] * fixo_n_i8])
        row.append(
            ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-100%', n['Value'].values[0], fixo_n_i1,
             inflacao['CORRECAO'].values[0] * fixo_n_i1, fixo_n_i1, inflacao['CORRECAO'].values[0] * fixo_n_i1])
        row.append(
            ['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-CQ50%', n['Value'].values[0], fixo_n_cq5,
             inflacao['CORRECAO'].values[0] * fixo_n_cq5, fixo_n_cq5, inflacao['CORRECAO'].values[0] * fixo_n_cq5])
        row.append(['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-y', n['Value'].values[0], fixo_n_ly,
                    inflacao['CORRECAO'].values[0] * fixo_n_ly, fixo_n_ly,
                    inflacao['CORRECAO'].values[0] * fixo_n_ly])

    df_tabmkt_24_45 = pd.DataFrame(row, columns=['Cenario', 'Ano', 'Mes', 'Submercado', 'Energia', 'PLD', 'Fixo',
                                                 'Fixo_infl', 'Pos', 'Pos_infl'])
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_mkt_anual', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('tab_mkt_anual - exec_time  = {} seconds '.format(exec_time.seconds))
    return df_tabmkt_24_45

# def tab_mkt_anual(tabMarcacao_df,df_inflacao_cenario,data_ini,data_fim):
#     init_time = datetime.datetime.now()
#     row = []
#     dates = pd.date_range(data_ini,data_fim, freq='MS').strftime("%Y/%m").tolist()
#     for data in dates:
#         data_split = data.split('/')
#         data_inteira = data + "/01"
#         data_inteira = data_inteira.replace('/','-')

#         se = tabMarcacao_df[(tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD Implicito SE/CO") & (tabMarcacao_df["Date"] == data_split[0])]
#         s = tabMarcacao_df[(tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD Implicito SUL") & (tabMarcacao_df["Date"] == data_split[0])]
#         ne = tabMarcacao_df[(tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD Implicito NE") & (tabMarcacao_df["Date"] == data_split[0])]
#         n = tabMarcacao_df[(tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "PLD Implicito NORTE") & (tabMarcacao_df["Date"] == data_split[0])]

#         # Swap
#         i0 = tabMarcacao_df[(tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP I0") & (tabMarcacao_df["Date"] == data_split[0])]
#         i5 = tabMarcacao_df[(tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP I5") & (tabMarcacao_df["Date"] == data_split[0])]
#         i8 = tabMarcacao_df[(tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP I8") & (tabMarcacao_df["Date"] == data_split[0])]
#         i1 = tabMarcacao_df[(tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP I1") & (tabMarcacao_df["Date"] == data_split[0])]
#         cq5 = tabMarcacao_df[(tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP CQ5") & (tabMarcacao_df["Date"] == data_split[0])]
#         ly = tabMarcacao_df[(tabMarcacao_df["Cenario"] == "Base") & (tabMarcacao_df["CONVENTIONAL"] == "SWAP Iy") & (tabMarcacao_df["Date"] == data_split[0])]

#     # inflação
#         if int(data_split[1]) < 10:
#             aux = data_split[1]
#             aux = aux.replace("0","")
#             inflacao = df_inflacao_cenario[(df_inflacao_cenario["CENARIO"] == "Base") & (df_inflacao_cenario["MES"] == int(aux)) & (df_inflacao_cenario["ANO"] == int(data_split[0]))]
#         else:
#             inflacao = df_inflacao_cenario[(df_inflacao_cenario["CENARIO"] == "Base") & (df_inflacao_cenario["MES"] == int(data_split[1])) & (df_inflacao_cenario["ANO"] == int(data_split[0]))]

#     # Fixo (PLD + Swap)
#     # SE
#         fixo_se_i0 = se['Value'].values[0]+i0['Value'].values[0]
#         fixo_se_i5 = se['Value'].values[0]+i5['Value'].values[0]
#         fixo_se_i8 = se['Value'].values[0]+i8['Value'].values[0]
#         fixo_se_i1 = se['Value'].values[0]+i1['Value'].values[0]
#         fixo_se_cq5 = se['Value'].values[0]+cq5['Value'].values[0]
#         fixo_se_ly = se['Value'].values[0]+ly['Value'].values[0]
#     # S
#         fixo_s_i0 = s['Value'].values[0]+i0['Value'].values[0]
#         fixo_s_i5 = s['Value'].values[0]+i5['Value'].values[0]
#         fixo_s_i8 = s['Value'].values[0]+i8['Value'].values[0]
#         fixo_s_i1 = s['Value'].values[0]+i1['Value'].values[0]
#         fixo_s_cq5 = s['Value'].values[0]+cq5['Value'].values[0]
#         fixo_s_ly = s['Value'].values[0]+ly['Value'].values[0]
#     # NE
#         fixo_ne_i0 = ne['Value'].values[0]+i0['Value'].values[0]
#         fixo_ne_i5 = ne['Value'].values[0]+i5['Value'].values[0]
#         fixo_ne_i8 = ne['Value'].values[0]+i8['Value'].values[0]
#         fixo_ne_i1 = ne['Value'].values[0]+i1['Value'].values[0]
#         fixo_ne_cq5 = ne['Value'].values[0]+cq5['Value'].values[0]
#         fixo_ne_ly = ne['Value'].values[0]+ly['Value'].values[0]
#     # N
#         fixo_n_i0 = n['Value'].values[0]+i0['Value'].values[0]
#         fixo_n_i5 = n['Value'].values[0]+i5['Value'].values[0]
#         fixo_n_i8 = n['Value'].values[0]+i8['Value'].values[0]
#         fixo_n_i1 = n['Value'].values[0]+i1['Value'].values[0]
#         fixo_n_cq5 = n['Value'].values[0]+cq5['Value'].values[0]
#         fixo_n_ly = n['Value'].values[0]+ly['Value'].values[0]

#         row.append(['BASE', data_split[0], data_split[1], 'SE/CO', 'Convencional', se['Value'].values[0], se['Value'].values[0], inflacao['CORRECAO'].values[0] * se['Value'].values[0], se['Value'].values[0], inflacao['CORRECAO'].values[0] * se['Value'].values[0]])
#         row.append(['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-0%', se['Value'].values[0],fixo_se_i0 , inflacao['CORRECAO'].values[0] * fixo_se_i0, fixo_se_i0 , inflacao['CORRECAO'].values[0] * fixo_se_i0])
#         row.append(['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-50%', se['Value'].values[0], fixo_se_i5, inflacao['CORRECAO'].values[0] * fixo_se_i5, fixo_se_i5, inflacao['CORRECAO'].values[0] * fixo_se_i5])
#         row.append(['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-80%', se['Value'].values[0], fixo_se_i8, inflacao['CORRECAO'].values[0] * fixo_se_i8, fixo_se_i8, inflacao['CORRECAO'].values[0] * fixo_se_i8])
#         row.append(['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-100%', se['Value'].values[0], fixo_se_i1, inflacao['CORRECAO'].values[0] * fixo_se_i1, fixo_se_i1, inflacao['CORRECAO'].values[0] * fixo_se_i1])
#         row.append(['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-CQ50%', se['Value'].values[0], fixo_se_cq5, inflacao['CORRECAO'].values[0] * fixo_se_cq5, fixo_se_cq5, inflacao['CORRECAO'].values[0] * fixo_se_cq5])
#         row.append(['BASE', data_split[0], data_split[1], 'SE/CO', 'Incentivada-y', se['Value'].values[0], fixo_se_ly, inflacao['CORRECAO'].values[0] * fixo_se_ly, fixo_se_ly, inflacao['CORRECAO'].values[0] * fixo_se_ly])

#         row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Convencional', s['Value'].values[0], s['Value'].values[0], inflacao['CORRECAO'].values[0] * s['Value'].values[0], s['Value'].values[0], inflacao['CORRECAO'].values[0] * s['Value'].values[0]])
#         row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-0%', s['Value'].values[0],fixo_s_i0 , inflacao['CORRECAO'].values[0] * fixo_s_i0, fixo_s_i0 , inflacao['CORRECAO'].values[0] * fixo_s_i0])
#         row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-50%', s['Value'].values[0],fixo_s_i5 , inflacao['CORRECAO'].values[0] * fixo_s_i5, fixo_s_i5 , inflacao['CORRECAO'].values[0] * fixo_s_i5])
#         row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-80%', s['Value'].values[0],fixo_s_i8 , inflacao['CORRECAO'].values[0] * fixo_s_i8, fixo_s_i8 , inflacao['CORRECAO'].values[0] * fixo_s_i8])
#         row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-100%', s['Value'].values[0],fixo_s_i1 , inflacao['CORRECAO'].values[0] * fixo_s_i1, fixo_s_i1 , inflacao['CORRECAO'].values[0] * fixo_s_i1])
#         row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-CQ50%', s['Value'].values[0],fixo_s_cq5 , inflacao['CORRECAO'].values[0] * fixo_s_cq5, fixo_s_cq5 , inflacao['CORRECAO'].values[0] * fixo_s_cq5])
#         row.append(['BASE', data_split[0], data_split[1], 'SUL', 'Incentivada-y', s['Value'].values[0],fixo_s_ly , inflacao['CORRECAO'].values[0] * fixo_s_ly, fixo_s_ly , inflacao['CORRECAO'].values[0] * fixo_s_ly])

#         row.append(['BASE', data_split[0], data_split[1], 'NE', 'Convencional', ne['Value'].values[0], ne['Value'].values[0], inflacao['CORRECAO'].values[0] * ne['Value'].values[0], ne['Value'].values[0], inflacao['CORRECAO'].values[0] * ne['Value'].values[0]])
#         row.append(['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-0%', ne['Value'].values[0],fixo_ne_i0 , inflacao['CORRECAO'].values[0] * fixo_ne_i0, fixo_ne_i0 , inflacao['CORRECAO'].values[0] * fixo_ne_i0])
#         row.append(['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-50%', ne['Value'].values[0],fixo_ne_i5 , inflacao['CORRECAO'].values[0] * fixo_ne_i5, fixo_ne_i5 , inflacao['CORRECAO'].values[0] * fixo_ne_i5])
#         row.append(['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-80%', ne['Value'].values[0],fixo_ne_i8 , inflacao['CORRECAO'].values[0] * fixo_ne_i8, fixo_ne_i8 , inflacao['CORRECAO'].values[0] * fixo_ne_i8])
#         row.append(['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-100%', ne['Value'].values[0],fixo_ne_i1 , inflacao['CORRECAO'].values[0] * fixo_ne_i1, fixo_ne_i1 , inflacao['CORRECAO'].values[0] * fixo_ne_i1])
#         row.append(['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-CQ50%', ne['Value'].values[0],fixo_ne_cq5 , inflacao['CORRECAO'].values[0] * fixo_ne_cq5, fixo_ne_cq5 , inflacao['CORRECAO'].values[0] * fixo_ne_cq5])
#         row.append(['BASE', data_split[0], data_split[1], 'NE', 'Incentivada-y', ne['Value'].values[0],fixo_ne_ly , inflacao['CORRECAO'].values[0] * fixo_ne_ly, fixo_ne_ly , inflacao['CORRECAO'].values[0] * fixo_ne_ly])

#         row.append(['BASE', data_split[0], data_split[1], 'NORTE', 'Convencional', n['Value'].values[0], n['Value'].values[0], inflacao['CORRECAO'].values[0] * n['Value'].values[0], n['Value'].values[0], inflacao['CORRECAO'].values[0] * n['Value'].values[0]])
#         row.append(['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-0%', n['Value'].values[0],fixo_n_i0 , inflacao['CORRECAO'].values[0] * fixo_n_i0, fixo_n_i0 , inflacao['CORRECAO'].values[0] * fixo_n_i0])
#         row.append(['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-50%', n['Value'].values[0],fixo_n_i5 , inflacao['CORRECAO'].values[0] * fixo_n_i5, fixo_n_i5 , inflacao['CORRECAO'].values[0] * fixo_n_i5])
#         row.append(['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-80%', n['Value'].values[0],fixo_n_i8 , inflacao['CORRECAO'].values[0] * fixo_n_i8, fixo_n_i8 , inflacao['CORRECAO'].values[0] * fixo_n_i8])
#         row.append(['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-100%', n['Value'].values[0],fixo_n_i1 , inflacao['CORRECAO'].values[0] * fixo_n_i1, fixo_n_i1 , inflacao['CORRECAO'].values[0] * fixo_n_i1])
#         row.append(['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-CQ50%', n['Value'].values[0],fixo_n_cq5 , inflacao['CORRECAO'].values[0] * fixo_n_cq5, fixo_n_cq5 , inflacao['CORRECAO'].values[0] * fixo_n_cq5])
#         row.append(['BASE', data_split[0], data_split[1], 'NORTE', 'Incentivada-y', n['Value'].values[0],fixo_n_ly , inflacao['CORRECAO'].values[0] * fixo_n_ly, fixo_n_ly , inflacao['CORRECAO'].values[0] * fixo_n_ly])

#     df_tabmkt_24_45 = pd.DataFrame(row,columns=['Cenario','Ano','Mes', 'Submercado','Energia','PLD','Fixo','Fixo_infl','Pos','Pos_infl'])
#     end_time = datetime.datetime.now()
#     exec_time = end_time - init_time
#     tempo_exec = []
#     tempo_exec.append(['tab_mkt_anual',exec_time.seconds])
#     df_tempo_exec = pd.DataFrame(tempo_exec,columns=['job','time'])
#     tempo_exec_to_azure(df_tempo_exec)
#     print ('tab_mkt_anual - exec_time  = {} seconds '.format( exec_time.seconds))
#     return df_tabmkt_24_45


def tab_mkt_to_azure(df_tab_mkt):
    init_time = datetime.datetime.now()
    from sqlalchemy import create_engine
    engine = create_engine(credenciais)
    df_tab_mkt.to_sql(name='tabmkt', con=engine, if_exists='replace')
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_mkt_to_azure', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('tab_mkt_to_azure - exec_time  = {} seconds '.format(exec_time.seconds))


def tab_contratos_nova():
    # server = "192.168.0.22"
    # database = "WBC_ENERGY_DB"
    # username = "usr_matrix"
    # password = "matrix_usr"

    # server = "tcp:sql-ufo-matrix.848240e609d1.database.windows.net"
    # database = "Ufo_Etrm_matrix-hml"
    # username = "usr_query_relatorios"
    # password = "Hu#N%?dr8$wnmsdC"

    server = "10.2.4.8"
    database = "Ufo_Etrm_matrix"
    username = "usr_query_relatorios"
    password = "Hu#N%?dr8$wnmsdC"

    init_time = datetime.datetime.now()
    cnxn = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    #     df = pd.read_sql('SELECT * FROM DBO.VW_CE_EXPORTAR_CONTRATOS_MATRIX_NEW;', cnxn)
    #     return df
    df_contratos = pd.read_sql("""
    Select
    a.Contraparte_CNPJ, 
    a.Parte_CNPJ, 
    a.Proprietaria_CNPJ, 
    a.Suprimento_Inicio, 
    a.Suprimento_termino,
    a.Perfil_CCEE_Parte, 
    a.ID_Contraparte, 
    a.Contraparte_nome_fantasia,
    a.Sigla_CCEE_Contraparte, 
    Submercado = Case
                    When a.Submercado = 'SE/CO' then 'SE/CO'
                    when a.Submercado = 'Sul' then 'SUL'
                    when a.Submercado = 'N' then 'NORTE'
                    when a.Submercado = 'NE' then 'NE'
                    Else ' Submercado Invalido'
                    End, 
    a.Codigo_CCEE, 
    a.Codigo_WBC, 
    a.Data_criacao as 'Data_Criacao',
    Convert(varchar(10),a.Data_publicacao, 103) as 'Data_Publicacao',
    a.Usuario_ultima_alteracao,
    a.Usuario_proposta,
    a.Numero_proposta,
    a.Ano as 'Ano_Supri',
    a.Mes as 'Mes_Supri',
    a.Volume_medio_contratado,
    a.Quant_Contratada,
    a.Quant_Sazonalizada,
    a.Quant_Solicitada,
    a.QuantAtualizada,
    a.FlexLimite_sazonalizacaoMin, 
    a.FlexLimite_sazonalizacaoMax,
    a.FlexibilidadeMensalMin,
    a.FlexibilidadeMensalMax,
    a.FlexLimite_modulacaoMin,
    a.FlexLimite_modulacaoMax,
    Regra_Preco = Case
                    When a.Regra_Preco = 'Regra padrão' then 'Fixed'
                    When a.Regra_Preco = 'PLD + Ágio' then 'Pos'
                    When a.Regra_Preco = 'PLD + Ágio%' then 'Pos'
                    Else 'Regra Desconhecida'
                    End,
    Preco_MWh = Case
                       When (isnumeric(Convert(Varchar(14),a.Valor))=0) then 0
                       Else Convert (Decimal(14,2), Convert(Varchar(14),[dbo].[Remover_caracter_especial](a.Valor)))
                       End,
    Valor_Reajustado = Case
                       When (isnumeric(Convert(Varchar(14),a.ValorReajustado))=0) then 0
                       Else Convert (Decimal(14,6), Convert(Varchar(14),a.ValorReajustado))
                       End,
    Spread  = Case
                       When (isnumeric(Convert(Varchar(14),a.Form_Agio))=0) then 0
                       Else Convert (Decimal(14,2), Convert(Varchar(14),a.Form_Agio))
                       End,
    a.Reajuste_Data_primeiro_reajuste,
    a.Reajuste_Data_base,
    a.Reajuste_Periodicidade,
    a.Reajuste_referencia,
    Reajuste_indice_economico = Case
                      When a.Reajuste_indice_economico = 'IPCA (IBGE)' Then 'IPCA'
                      When a.Reajuste_indice_economico = 'IGP-M (FGV)' Then 'IGPM'
                      Else 'NO IDX'
                      end,
    a.Multa_juros_indice,
    a.Multa_juros_multa,
    a.Multa_juros_juros,
    a.Possui_garantia,
    a.Garantia_inicio_geral,
    a.Garantia_termino_geral,
    a.Garantia_apresentacao_geral,
    Pagamento_Vinculado = Case
               When a.Pagamento_Vinculado = 'Pagamento independente do registro na CCEE' then
                                          'Payment regardless of energy register'
               When a.Pagamento_Vinculado = 'Pagamento contra registro na CCEE' then
                                          'Payment upon energy register'
               When a.Pagamento_Vinculado = 'Registro na CCEE contra pagamento' then
                                          'Energy register upon payment'
               When a.Pagamento_Vinculado = 'Registro antecipado' then
                                          'Energy register in advance'
               End,
    a.Situacao_backoffice,
    a.Situacao_faturamento_backoffice,
    a.Valor_TRU,
    a.Condicao_pagto,
    a.Sigla_perfil_vendedor,
    a.Incentivado,
    a.Situacao_publicacao,
    a.Portfolio_Vendedor,
    a.Portfolio_Comprador,
    a.Data_emissao_fatura,
    a.Valor_Escriturado,
    Rateio_efetuado = Case 
                When a.Rateio_efetuado = '1' Then 'Sim'
                When a.Rateio_efetuado = '0' Then 'Não'
            End,
    Flex = Case   
                    When a.FlexLimite_sazonalizacaoMin <> '0.000' Then 'Sim'
                    When a.FlexLimite_sazonalizacaoMax  <> '0.000' Then 'Sim'
                    When a.FlexibilidadeMensalMin <> '0.000' Then 'Sim'
                    When a.FlexibilidadeMensalMax <> '0.000' Then 'Sim'
                    When a.FlexLimite_modulacaoMin <> '0.000' Then 'Sim'
                    When a.FlexLimite_modulacaoMax <> '0.000' Then 'Sim'
                    Else 'Não'
                    End, 
    a.Quant_Modulada as 'Vol_Modulado',
    tipo_energia = Case
                      When a.Sigla_CCEE = 'MATRIX COM'     Then 'Convencional'
                      When a.Sigla_CCEE = 'MATRIX COM I0'  Then 'Incentivada-0%'
                      When a.Sigla_CCEE = 'MATRIX COM I5'  Then 'Incentivada-50%'
                      When a.Sigla_CCEE = 'MATRIX COM I1'  Then 'Incentivada-100%'
                      When a.Sigla_CCEE = 'MATRIX COM I8'  Then 'Incentivada-80%'
                      When a.Sigla_CCEE = 'MATRIX COM CQ5' Then 'Incentivada-CQ50%'
                      When a.Sigla_CCEE = 'CINERGY COM'     Then 'Convencional'
                      When a.Sigla_CCEE = 'CINERGY COM I0'  Then 'Incentivada-0%'
                      When a.Sigla_CCEE = 'CINERGY COM I5'  Then 'Incentivada-50%'
                      When a.Sigla_CCEE = 'CINERGY COM I1'  Then 'Incentivada-100%'
                      When a.Sigla_CCEE = 'CINERGY COM I8'  Then 'Incentivada-80%'
                      When a.Sigla_CCEE = 'BISMUT COM'     Then 'Convencional'
                      When a.Sigla_CCEE = 'BISMUT COM I0'  Then 'Incentivada-0%'
                      When a.Sigla_CCEE = 'BISMUT COM I5'  Then 'Incentivada-50%'
                      When a.Sigla_CCEE = 'BISMUT COM I1'  Then 'Incentivada-100%'
                      When a.Sigla_CCEE = 'BISMUT COM I8'  Then 'Incentivada-80%'
                      When a.Sigla_CCEE = 'BISMUT COM.'     Then 'Convencional'
                      When a.Sigla_CCEE = 'BISMUT COM I0.'  Then 'Incentivada-0%'
                      When a.Sigla_CCEE = 'BISMUT COM I5.'  Then 'Incentivada-50%'
                      When a.Sigla_CCEE = 'BISMUT COM I1.'  Then 'Incentivada-100%'
                      When a.Sigla_CCEE = 'BISMUT COM I8.'  Then 'Incentivada-80%'
                      When a.Sigla_CCEE = 'CRIPTON'     Then 'Convencional'
                      When a.Sigla_CCEE = 'CRIPTON I0'  Then 'Incentivada-0%'
                      When a.Sigla_CCEE = 'CRIPTON I5'  Then 'Incentivada-50%'
                      When a.Sigla_CCEE = 'CRIPTON I1'  Then 'Incentivada-100%'
                      When a.Sigla_CCEE = 'CRIPTON I8'  Then 'Incentivada-80%'
                      Else 'Enegia Desconhecida'
                      End,
    Horas_Mes = Case
               When a.Mes in (1,3,5,7,8,12) Then 744                     /* Horas do mes = 31*24 */
               When a.Mes in (4,6,9) Then 720                            /* Horas do mes = 30*24*/
               When a.Ano <= 2017 and a.Mes = 10 then 744 - 1              /* Horario de verao = 31*24 -1 */
               When a.Ano <= 2017 and a.Mes = 11 then 720
               When a.Ano <= 2017 and a.Ano % 4 = 0 and a.Mes = 2 Then 697   /* Ano Bissexto e Horario Verao mes = 29*24 +1 */
               When a.Ano <= 2017 and a.Ano % 4 <> 0 and a.Mes = 2 Then 673  /* Horario de verao 28*24 + 1 */
               When a.Ano = 2018 and a.Mes = 10 then 744                   /* mes = 31 *24 */
               When a.Ano = 2018 and a.Mes = 11 then 720 - 1               /* Horario de verao 30*24 + 1 */
               When a.Ano = 2018 and a.Mes = 2 Then 673                    /* Horario de verao 28*24 + 1 */
               When a.Ano >= 2019 and a.Mes = 10 then 744                  /* mes = 31 *24 */
               When a.Ano >= 2019 and a.Mes = 11 then 720                  /* mes =  30*24  */
               When a.Ano = 2019 and a.Mes = 2 Then 673                    /* Horario de verao 28*24 + 1 */
               When a.Ano > 2019 and a.Ano % 4 = 0 and a.Mes = 2 Then 696    /* Ano Bissexto e Horario Verao mes = 29*24 */
               When a.Ano > 2019 and a.Ano % 4 <> 0 and a.Mes = 2 Then 672   /* Horario de verao 28*24  */
               Else 0                                                          /* erro  */
               End,
    Convert(varchar(10), a.Data_Parcela_3,103) as 'Acerto',
    Condicao = Case
                      when  a.Pagamento_vinculado = 'Registro na ccee contra pagamento' Then 2
                      Else 1
                      End,
    Titulo_Liq = Case
                      when    a.Data_Parcela_1 < CONVERT(CHAR(10), GETDATE(),112) and
                              a.Data_Parcela_2 < CONVERT(CHAR(10), GETDATE(),112) and
                              a.Data_Parcela_3 < CONVERT(CHAR(10), GETDATE(),112) then 'Sim'
                      Else 'Nao'
                      end,
    a.Data_Parcela_1 as 'Vencimento',
    Vol_MWh_Min = case
                               When a.Quant_Sazonalizada Is NULL Then a.Quant_Contratada - (a.Quant_Contratada * (Convert(Numeric(30,3),a.FlexibilidadeMensalMin)/100))
                               When a.Quant_Sazonalizada <> 0 then a.Quant_Sazonalizada - (a.Quant_Sazonalizada * (Convert(Decimal(30,3),a.FlexibilidadeMensalMin)/100))
                               When a.Quant_Contratada Is NULL Then 0
                               When a.Quant_Contratada <> 0 then a.Quant_Contratada - (a.Quant_Contratada * (Convert(Numeric(30,3),a.FlexibilidadeMensalMin)/100))
                               Else 0
                           end,
    Vol_MWh_Max = case
                               When a.Quant_Sazonalizada Is NULL Then a.Quant_Contratada + (a.Quant_Contratada * (Convert(Numeric(30,3),a.FlexibilidadeMensalMin)/100))
                               When a.Quant_Sazonalizada <> 0 then a.Quant_Sazonalizada + (a.Quant_Sazonalizada * (Convert(Decimal(30,3),a.FlexibilidadeMensalMin)/100))
                               When a.Quant_Contratada Is NULL Then 0
                               When a.Quant_Contratada <> 0 then a.Quant_Contratada + (a.Quant_Contratada * (Convert(Numeric(30,3),a.FlexibilidadeMensalMin)/100))
                               Else 0
                         end,
    b.Garantia_valor_calculado as 'Gar_Val_Calculado',
    
    a.Form_percentual_agio as 'PLD_Agio_Porcentagem',
    Rateio,
	boleta_mae_ou_nao_rateio = case
        When ISNULL(a.Rateio,'0') = '0' or trim(a.Rateio) = '' then 'S/l'
        When a.Rateio = 'Não' then 'NAO RATEIO'
        When ISNULL(a.Nr_contrato_vinculado,'0') = '0' and a.Rateio = 'Sim' then 'MAE'
    else 'FILHA' End,
    a.Movimentacao,
    a.Nr_contrato_vinculado,
    a.Numero_referencia_contrato as ' Referencia',
    a.Situacao,
    a.Situacao_publicacao

    from DBO.VW_CE_EXPORTAR_CONTRATOS_MATRIX_NEW AS a
    left join DBO.VW_CE_EXPORTAR_CONTRATOS_GARANTIA  b on  b.Codigo_WBC = a.Codigo_WBC and Year(b.Garantia_periodo_inicial_calculo) = a.Ano
    where a.nCdEmpresaProprietaria in (3,158,10848, 11356, 11796) and a.Tipo_contrato = 'Bilateral' and a.Situacao = 'Publicado' and a.Ano >= '2021' and a.Portfolio_Vendedor <> 'Cinergy' and a.Portfolio_Comprador <> 'Cinergy'
    """, cnxn)

#(a.Rateio = 'Não' or (a.Rateio = 'Sim' and a.Nr_contrato_vinculado = NULL)) and
    df_contratos.dropna(subset=['Contraparte_CNPJ'], inplace=True)
    contratos_df = df_contratos
    contratos_df = contratos_df.replace(np.nan, 0)
    contratos_df = contratos_df.replace({pd.NaT: 0})
    #     contratos_df.fillna('', inplace=True)
    #     contratos_df.replace(np.NaN, '', inplace=True)

    contratos_df['Ano_Supri'] = contratos_df['Ano_Supri'].map(int)
    contratos_df['Mes_Supri'] = contratos_df['Mes_Supri'].map(int)
    contratos_df['Codigo_WBC'] = contratos_df['Codigo_WBC'].map(int)
    contratos_df['Mes_Supri'] = contratos_df.Mes_Supri.map("{:02}".format)
    contratos_df['Chave'] = ('BASE') + contratos_df['Ano_Supri'].map(str) + contratos_df['Mes_Supri'].map(str) + \
                            contratos_df['Submercado'] + contratos_df['tipo_energia']
    contratos_df['Vol_MWm'] = contratos_df['QuantAtualizada'] / contratos_df['Horas_Mes']
    contratos_df['Vol_MWm_Compra'] = np.where(contratos_df['Movimentacao'] == 'Compra', contratos_df['Vol_MWm'], 0)
    contratos_df['Vol_MWm_Venda'] = np.where(contratos_df['Movimentacao'] == 'Venda', contratos_df['Vol_MWm'], 0)
    contratos_df['Year_Month'] = contratos_df['Ano_Supri'].map(str) + '-' + contratos_df['Mes_Supri'].map(str)
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    #     tempo_exec = []
    #     tempo_exec.append(['tab_contratos_wbc', exec_time.seconds])
    #     df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    #     tempo_exec_to_azure(df_tempo_exec)
    print('tab_contratos_wbc - exec_time  = {} seconds '.format(exec_time.seconds))
    return contratos_df


def tab_contratos_nova_metricas(df1, df2, df_final):
    init_time = datetime.datetime.now()
    from dateutil.relativedelta import relativedelta
    import datetime as dt

    contratos_df = df1
    df_tab_mkt = df2
    df_juros = df_final

    #     contratos_df2 = contratos_df
    df3 = contratos_df.merge(df_tab_mkt, left_on='Chave', right_on='Chave', how='left')

    df3['Valor_PLD'] = df3['PLD']
    df3['Valor_MWh_Calc'] = np.where(df3['Regra_Preco'] == 'Pos', df3['Valor_PLD'] + df3['Spread'],
                                     df3['Valor_Reajustado'])
    df3['Valor_MKT'] = np.where(df3['Regra_Preco'] == 'Fixed', df3['Fixo_infl'], df3['Pos_infl'])
    df3['Notional'] = np.where(df3['Acerto'] == 'Acerto', df3['QuantAtualizada'] * df3['Valor_MWh_Calc'],
                               (df3['QuantAtualizada'] * df3['Valor_MWh_Calc']).abs())
    # Se compra, MtM_Value = QuantAtualizada*(Valor_MKT - Valor_Reajustado)
    df3['MTM_Value'] = np.where(df3['Movimentacao'] == 'Compra',
                                ((df3['Valor_MKT'] - df3['Valor_MWh_Calc']) * df3['QuantAtualizada']), (df3['Valor_MWh_Calc'] - df3['Valor_MKT']) * df3['QuantAtualizada'])
    # MtM_Value_venda = QuantAtualizada*(Valor_Reajustado – Valor_MKT)
    # df3['MTM_Value'] = np.where(df3['Movimentacao'] == 'Venda',
    #                             ((df3['Valor_MWh_Calc'] - df3['Valor_MKT']) * df3['QuantAtualizada']), 0)
    #  Notional = QuantAtualizada * Valor_Reajustado
    df3['Notional_MWm'] = np.where(df3['Regra_Preco'] == 'Fixed', df3['Valor_MWh_Calc'] * df3['Vol_MWm'],
                                   df3['Spread'] * df3['Vol_MWm'])

    df3['ChaveJuros'] = df3['Year_Month'] + '-01'
    #matheus
    df3['ChaveJuros'] = pd.to_datetime(df3['ChaveJuros'])
    df3['ChaveJuros'] = df3['ChaveJuros'] + pd.DateOffset(months=1)

    df3['ChaveJuros'] = df3['ChaveJuros'].astype(str)
    df4 = df3.merge(df_juros, left_on='ChaveJuros', right_on='AnoMes', how='left')

    df4['Vencimento_NPV_MTM'] = df4['Vencimento'].apply(lambda dt: dt.replace(day=1))
    df4['Vencimento_NPV_MTM'] = df4['Vencimento_NPV_MTM'].astype(str)
    df5 = df4.merge(df_juros, left_on='Vencimento_NPV_MTM', right_on='AnoMes', how='left')

    df5['data_w1'] = df5['Year_Month'] + '-01'
    df5['data_w1'] = pd.to_datetime(df5['data_w1'])
    f = lambda x: x['data_w1'] + relativedelta(months=1)
    df5['data_w1'] = df5.apply(f, axis=1)
    df5['data_w1'] = df5['data_w1'].astype(str)
    df6 = df5.merge(df_juros, left_on='data_w1', right_on='AnoMes', how='left')

    # df6['NPV_MTM'] = np.where(df6['Acerto'] == 'Acerto_MTM', df6['MTM_Value'] / df6['Fator_Reducao_x'],
    #                           np.where(df6['Movimentacao'] == 'Venda', df6['Notional'] /
    #                                    (df6['Fator_Reducao_x'] - (
    #                                                (df6['Valor_MKT'] * df6['QuantAtualizada']) / df6['Fator_Reducao'])),
    #                                    df6['Fator_Reducao_y']))

    df6['NPV_MTM'] = np.where(df6['Acerto'] == 'Acerto_MTM', df6['MTM_Value'] / df6['Fator_Reducao_y'],
                              np.where(df6['Movimentacao'] == 'Venda', df6['Notional'] /
                                       df6['Fator_Reducao_y'] - (
                                               (df6['Valor_MKT'] * df6['QuantAtualizada']) / df6['Fator_Reducao']),
                                       ((df6['Valor_MKT'] * df6['QuantAtualizada']) / df6['Fator_Reducao']) - (df6[
                                  'Notional'] / (df6['Fator_Reducao_y']))))

    df6['portfolio'] = np.where(df6['Movimentacao'] == 'Venda', df6['Portfolio_Vendedor'],
                                df6['Portfolio_Comprador'])

    df6['portfolio'] = np.where(df6['portfolio'] == '', 'Matrix',df6['portfolio'])

    # LIMPAR O QUE NÃO FOR PURCHASE/SALE
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_contratos_metricas', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('tab_contratos_metricas - exec_time  = {} seconds '.format(exec_time.seconds))
    return df6


def tab_contratos_nova_to_csv(df, PATH):
    init_time = datetime.datetime.now()
    df.to_csv(PATH)
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_contratos_to_csv', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('tab_contratos_to_csv - exec_time  = {} seconds '.format(exec_time.seconds))


def tab_contratos_nova_to_azure(df6):
    init_time = datetime.datetime.now()
    from sqlalchemy import create_engine
    engine = create_engine(credenciais)
    df6.to_sql(name='contratos', con=engine, if_exists='replace', index=False)
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_contratos_to_azure', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('tab_contratos_to_azure - exec_time  = {} seconds '.format(exec_time.seconds))


def tab_juros(PATH):
    init_time = datetime.datetime.now()
    df_juros = pd.read_excel(PATH, sheet_name="Juros")
    df_juros['AnoMes'] = df_juros['AnoMes'].astype(str)
    # df_juros['AnoMes'] = df_juros['AnoMes'].replace(' 00:00:00','')
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_juros', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('tab_juros - exec_time  = {} seconds '.format(exec_time.seconds))
    return df_juros


def tab_juros_to_azure(df):
    init_time = datetime.datetime.now()
    from sqlalchemy import create_engine
    engine = create_engine(credenciais)
    df.to_sql(name='juros', con=engine, if_exists='replace', index=False)
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['tab_juros_to_azure', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('tab_juros_to_azure - exec_time  = {} seconds '.format(exec_time.seconds))


def acertos(PATH):
    init_time = datetime.datetime.now()
    df_acertos = pd.read_excel(PATH,
                               sheet_name="Acertos"
                               )
    #     df_acertos = df_acertos.drop(df_acertos.columns[[10,11]], axis=1, inplace=True)
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['acertos', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('acertos - exec_time  = {} seconds '.format(exec_time.seconds))
    return df_acertos


def acertos_to_azure(df_acertos):
    init_time = datetime.datetime.now()
    from sqlalchemy import create_engine
    engine = create_engine(
        credenciais)
    df_acertos.to_sql(name='acertos', con=engine, if_exists='replace', index=False)
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['acertos_to_azure', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('acertos_to_azure - exec_time  = {} seconds '.format(exec_time.seconds))


def acertos_to_contratos(df_acertos, df_contratos):
    init_time = datetime.datetime.now()
    contrato_com_acertos_df_1, contrato_com_acertos_df_2 = df_contratos.align(df_acertos, join='outer', axis=1)
    contrato_com_acertos_df = pd.concat([contrato_com_acertos_df_1, contrato_com_acertos_df_2])
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['acertos_to_contratos', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('acertos_to_contratos - exec_time  = {} seconds '.format(exec_time.seconds))
    return contrato_com_acertos_df


def base_contratos(df):
    init_time = datetime.datetime.now()
    print('Iniciado insert de dados na tabela de contratos')
    from sqlalchemy import create_engine
    engine = create_engine(credenciais)
    df.to_sql(name='contratos', con=engine, if_exists='replace', index=False)
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['contratos_diario', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('     -contratos_diario - exec_time  = {} seconds '.format(exec_time.seconds))


def base_contratos_final(df):
    init_time = datetime.datetime.now()
    print('Iniciado insert de dados na tabela historica de contratos')
    from sqlalchemy import create_engine
    engine = create_engine(credenciais)
    # df = df.dropna(axis=1, how='all')
    df = df.dropna(axis=0, how='all')
    df.to_sql(name='contratos_historico', con=engine, if_exists='append', index=False)
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['contratos_historico', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('    -contratos_historico - exec_time  = {} seconds '.format(exec_time.seconds))


def base_contratos_wbc(df):
    init_time = datetime.datetime.now()
    print('Iniciado insert dos dados lidos do WBC')
    from sqlalchemy import create_engine
    engine = create_engine(credenciais)
    df.to_sql(name='contratos_wbc', con=engine, if_exists='replace', index=False)
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['Contratos_WBC', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('    -Contratos_WBC - exec_time  = {} seconds '.format(exec_time.seconds))


def read_sql_contratos(tipo_leitura=1):
    tipo_leitura = tipo_leitura
    init_time = datetime.datetime.now()
    print('Iniciado leitura de Contratos')
    from sqlalchemy import create_engine
    engine = create_engine(credenciais)
    if tipo_leitura == 1:
        df = pd.read_sql(sql='Select * from contratos_wbc', con=engine)
    else:
        df = pd.read_sql(sql='Select * from contratos', con=engine)
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['leitura_contratos_azure', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)
    print('     -Contratos com métricas - exec_time  = {} seconds '.format(exec_time.seconds))
    return df


def to_csv(df, PATH):
    init_time = datetime.datetime.now()
    df.to_csv(PATH, sep=';')
    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    print('to_csv - exec_time  = {} seconds '.format(exec_time.seconds))


# LER CONTRATOS WBC
def ler_wbc():
    init_time = datetime.datetime.now()
    print('ler_WBC()')
    df_contratos = tab_contratos_nova()
    to_csv(df_contratos,
           CONTRATOS_WBC_CSV)
    base_contratos_wbc(df_contratos)
    montar_tabelas_bases_e_metricas(df_contratos, data_ini_mensal_mkt='2021-01-01', data_fim_mensal_mkt='2023-12-01',
                                    data_ini_anual_mkt='2024-01-01', data_fim_anual_mkt='2045-12-01',
                                    data_reajuste_pld=0)

    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['ler_wbc', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)


def sincronizar():
    init_time = datetime.datetime.now()
    print('sincronizar()')
    df_contratos = read_sql_contratos(1)
    montar_tabelas_bases_e_metricas(df_contratos, data_ini_mensal_mkt='2021-01-01', data_fim_mensal_mkt='2023-12-01',
                                    data_ini_anual_mkt='2024-01-01', data_fim_anual_mkt='2045-12-01',
                                    data_reajuste_pld=0, csv=2)

    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['sincronizar', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)


def sincronizar_csv():
    init_time = datetime.datetime.now()
    print('sincronizar()')
    df_contratos = pd.read_csv(
        CONTRATOS_WBC_CSV)
    df_contratos['Vencimento'] = pd.to_datetime(df_contratos['Vencimento'])
    montar_tabelas_bases_e_metricas(df_contratos, data_ini_mensal_mkt='2021-01-01', data_fim_mensal_mkt='2023-12-01',
                                    data_ini_anual_mkt='2024-01-01', data_fim_anual_mkt='2045-12-01',
                                    data_reajuste_pld=0, csv=1)

    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['sincronizar_csv', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)


def montar_tabelas_bases_e_metricas(df_contratos, data_ini_mensal_mkt='2021-01-01', data_fim_mensal_mkt='2023-12-01',
                                    data_ini_anual_mkt='2024-01-01', data_fim_anual_mkt='2045-12-01',
                                    data_reajuste_pld=0, csv=2):
    init_time = datetime.datetime.now()
    print('montar_tabelas_bases_e_metricas()')
    csv = csv
    df_contratos = df_contratos
    # JUROS
    df_juros = tab_juros(PATH)
    tab_juros_to_azure(df_juros)

    # MARCAÇÃO
    tabMarcacao_df = tab_marcacao(PATH)
    tab_marcacao_to_azure(tabMarcacao_df)

    # INFLACAO
    df_inflacao = tab_inflacao(PATH)
    tab_inflacao_to_azure(df_inflacao)

    # INFLACAO CENARIO
    df_inflacao_cenario = tab_inflacao_cenario(PATH)
    tab_inflacao_cenario_to_azure(df_inflacao_cenario)

    # MKT
    def round_2(x):
        try:
            return round(x, 2)
        except:
            return x

    df_tab_mkt_mensal = tab_mkt_mensal(tabMarcacao_df, df_inflacao_cenario, data_ini_mensal_mkt, data_fim_mensal_mkt)
    df_tab_mkt_anual = tab_mkt_anual(tabMarcacao_df, df_inflacao_cenario, data_ini_anual_mkt, data_fim_anual_mkt)
    df_tab_mkt = pd.concat([df_tab_mkt_mensal, df_tab_mkt_anual])
    df_tab_mkt['Chave'] = df_tab_mkt['Cenario'].map(str) + df_tab_mkt['Ano'].map(str) + df_tab_mkt['Mes'].map(str) + \
                          df_tab_mkt['Submercado'] + df_tab_mkt['Energia']
    df_tab_mkt['PLD'] = df_tab_mkt['PLD'].apply(round_2)
    df_tab_mkt['Fixo'] = df_tab_mkt['Fixo'].apply(round_2)
    df_tab_mkt['Fixo_infl'] = df_tab_mkt['Fixo_infl'].apply(round_2)
    df_tab_mkt['Pos'] = df_tab_mkt['Pos'].apply(round_2)
    df_tab_mkt['Pos_infl'] = df_tab_mkt['Pos_infl'].apply(round_2)
    # 'Fixo','Fixo_infl','Pos','Pos_infl'
    tab_mkt_to_azure(df_tab_mkt)

    # CONTRATOS COM MÉTRICAS
    df_contratos_2 = tab_contratos_nova_metricas(df_contratos, df_tab_mkt, df_juros)
    # tab_contratos_nova_to_csv(df_contratos, PATH)

    # ACERTOS NA TABELA DE CONTRATOS
    df_acertos = acertos(PATH)
    acertos_to_azure(df_acertos)
    df_contratos_3 = acertos_to_contratos(df_acertos, df_contratos_2)

    df_contratos_final = df_contratos_3

    del df_contratos_final['Volume referência (MWh)']
    del df_contratos_final['Preço referência (R$/MWh)']
    del df_contratos_final['AnoMes_x']
    del df_contratos_final['AnoMes_y']
    # del df_contratos_final['Fator_Reducao_x']
    # del df_contratos_final['Fator_Reducao_y']
    del df_contratos_final['Submercado_y']
    # del df_contratos_final['data_w1']
    del df_contratos_final['ChaveJuros']
    del df_contratos_final['Ano']
    del df_contratos_final['Mes']
    del df_contratos_final['Energia']
    df_contratos_final = df_contratos_final.rename(index=str).rename(columns={'Submercado_x': 'Submercado'})
    df_contratos_final = df_contratos_final.rename(index=str).rename(columns={'Ano_Supri': 'Ano'})
    df_contratos_final = df_contratos_final.rename(index=str).rename(columns={'Mes_Supri': 'Mes'})

    # GUARDAR DADOS FINAIS
    if csv == 2:
        base_contratos(df_contratos_final)

    to_csv(df_contratos_final,
           CONTRATOS_CSV)
    to_csv(tabMarcacao_df,
           MARCACAO_CSV)
    to_csv(df_inflacao_cenario,
           INFLACAO_CSV)
    to_csv(df_tab_mkt,
           MKT_CSV)
    to_csv(df_acertos,
           ACERTOS_CSV)
    to_csv(df_juros,
           JUROS_CSV)

    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['montar_tabelas_bases_e_metricas', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)


def guarda_dados_historicos():
    init_time = datetime.datetime.now()
    print('guarda_dados_historicos()')

    df = read_sql_contratos(2)
    base_contratos_final(df)

    end_time = datetime.datetime.now()
    exec_time = end_time - init_time
    tempo_exec = []
    tempo_exec.append(['guarda_dados_historicos', exec_time.seconds])
    df_tempo_exec = pd.DataFrame(tempo_exec, columns=['job', 'time'])
    tempo_exec_to_azure(df_tempo_exec)


def teste():
    import os
    print(os.environ['USERPROFILE'])


def ler_excel(PATH, sheet):
    import win32com.client as w3c

    xlapp = w3c.gencache.EnsureDispatch('Excel.Application')
    xlwb = xlapp.Workbooks.Open(PATH,
                                False, True, None)
    xlsheet = xlwb.Worksheets(sheet)
    a = xlsheet.Range("A1:FF100000").Value
    df = pd.DataFrame(a)
    return df


if __name__ == '__main__':
    ler_wbc()
    # sincronizar_csv()
    # sincronizar()