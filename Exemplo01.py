import polars as pl 
import gc # garbage collector
import os
import numpy as np
import matplotlib.pyplot as plt

from datetime import datetime
ENDERECO_DADOS = r'./dados/'

# try:     
#     print('Obtendo Dados...')
#     inicio = datetime.now()
#     lista_arquivos = []
#     #lista final dos arquivos de dados que vira do diretorio
#     lista_dir_arquivos = os.listdir(ENDERECO_DADOS)

#     for arquivo in lista_dir_arquivos:
#         if arquivo.endswith('.csv'):
#             lista_arquivos.append(arquivo)
#     print(lista_arquivos)

#     for arquivo in lista_arquivos:
#         print(f"Processando arquivo {arquivo}")
#         #leitura de cada um dos dataframes
#         df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')
    
#         if 'df_bolsa_familia' in locals():
#             df_bolsa_familia = pl.concat([df_bolsa_familia, df])
#         else: 
#             df_bolsa_familia = df

                
#         fim = datetime.now()
#         print(f"Tempo de execução: '{fim - inicio}")
#         print("Gravação do Arquivo Parquet realizado com sucesso")

#     df_bolsa_familia.write_parquet(ENDERECO_DADOS + 'BOLSA_FAMILIA_PARQUET2')
        
#     print(df_bolsa_familia)

#     # print(df.head())
#     # print(df.shape)
#     # print(df.columns)
#     # print(df.dtypes)

#     #limpar df da memoria
#     del df
#     # residuos da memoria
#     gc.collect()
    
# except ImportError as e:
#     print ("Erro ao obter dados: ", e)




#Lendo os dados do arquivo Parquet

try:
    print('\nIniciando leitura do arquivo parquet...')

    #Pegar o tempo inicial
    inicio = datetime.now()

    #df_bolsa_familia = pl.read_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
    df_bolsa_familia_plan = pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
    df_bolsa_familia = df_bolsa_familia_plan.collect()

    print(df_bolsa_familia.head())

    #Pegar o tempo final
    fim = datetime.now()

    print(f'Tempo de execução para a leitura do parquet: {fim - inicio}')
    print('\nArquivo parquet lido com sucesso!')

except Exception as e:
    print(f'Erro ao ler dados do parquet: {e}')


try:
    print('Vizualiar a distribuição dos valores das parcelas em um boxplot...')

    # Marcar a hora de inicio
    hora_inicio = datetime.now()

    # Criar um array Numpy com o valor da parcela
    array_valor_parcela = np.array(df_bolsa_familia['VALOR PARCELA'])

    # Criar um boxplot
    plt.boxplot(array_valor_parcela, vert=False)
    plt.title('Distribuição dos valores das parcelas')

    # Maracar a hora do termino
    hora_fim = datetime.now()

    plt.show()

    print(f'Tempo de execução: {hora_fim - hora_inicio}')
    print('Dados visualizados com sucesso!')

except Exception as e:
    print('Erro ao ler dados do parquet: {}')

