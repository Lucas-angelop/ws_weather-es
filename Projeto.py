# Databricks notebook source
# MAGIC %md
# MAGIC ### GERAL

# COMMAND ----------

# MAGIC %md
# MAGIC Desafio Autoglass  
# MAGIC - Tarefas  
# MAGIC [√] - ETAPA 01 : Consultar municípios do ES, gerar um data frame e criar uma temp view com esses dados.  
# MAGIC [√] - ETAPA 02 : Consultar dados do tempo para cada município, gerar um data frame e criar uma outra temp view.  
# MAGIC [√] - ETAPA 03 : Utilizar Spark SQL para gerar os data frames das Tabelas 1 e 2.  
# MAGIC [√] - ETAPA 04 : Exportar os data frames para CSV.  

# COMMAND ----------

# DOWNLOADS
# pip install unidecode

# COMMAND ----------

# IMPORTAÇÃO DE BIBLIOTECAS
import requests as req
from unidecode import unidecode
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETAPA 1    
# MAGIC REQUISIÇÃO API IBGE

# COMMAND ----------

# OBTENDO OS DADOS DO IBGE COM GET
cidades = req.get('https://servicodados.ibge.gov.br/api/v1/localidades/estados/32/municipios')
print('Retorno da API:', cidades.status_code)

# COMMAND ----------

# ATRIBUINDO RESULTADO EM UMA VARIAVEL
cidades = cidades.json()

# COMMAND ----------

# PREVIEW OBJETO JSON
cidades[11]

# COMMAND ----------

# VARIAVEIS PARA CONSTRUCAO DO DATAFRAME
qtde_cidades = len(cidades)
cidades_es = []
cidades_schema = 'ID INT,        \
                  CIDADE STRING'

# COMMAND ----------

# OBTENDO OS DADOS PARA O DATAFRAME
for i in range(qtde_cidades):
    cidades_es.append([(cidades[i]['id']), unidecode(cidades[i]['nome'])])

# COMMAND ----------

# CRIACAO DO DATAFRAME
cidades_es = spark.createDataFrame(cidades_es,schema)

# COMMAND ----------

# CRIANDO UM TEMP VIEW "CIDADES_ES"
cidades_es.createOrReplaceTempView("CIDADES_ES")

# COMMAND ----------

# SELECT DA TABELA PARA VERIFICAR CRIAÇÃO
spark.sql(
            "SELECT *\
               FROM CIDADES_ES\
              LIMIT 3"
         ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETAPA 2  
# MAGIC CONSULTAR DADOS DO CLIMA PARA AS CIDADES

# COMMAND ----------

# OBTENDO CIDADES DA TABELA PARA REQUEST NA API
cidades = spark.sql("\
                        SELECT C.CIDADE\
                          FROM CIDADES_ES C\
                    ")

# COMMAND ----------

# VERIFICANDO UM CIDADE
print(unidecode(cidades.collect()[70][0]))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### API M3O
# MAGIC - TENTATIVA 1
# MAGIC    - RETORNO 500 - INTERNAL SERVER ERROR  
# MAGIC    - NÃO CONSEGUI OBTER OS DADOS, TOKEN LIBERADO NÃO ESTAVA SENDO RECONHECIDO E NÃO CONSEGUI UTILIZAR
# MAGIC - TENTATIVA 2
# MAGIC    - RETORNO 200 - OK
# MAGIC    - COM OUTRO TOKEN CONSEGUI EXECUTAR

# COMMAND ----------

# PARAMETROS API
days = 5
content_type = "application/json"
token = "Bearer MDI2YWYzNTAtYTQyMi00OWM2LTgzMmItNTg1MzQ5NGRjNGJi"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### ANALISANDO API

# COMMAND ----------

# city = "Aguia Branca"
# weather = req.post("https://api.m3o.com/v1/weather/Forecast",
#           json = {'days': days, 'location': city},
#           headers = {"content-type" : content_type, "Authorization" : token})

# print('Retorno da API para a cidade', city, ':', weather.status_code)

# COMMAND ----------

# weather = weather.json()

# COMMAND ----------

# PREVIEW
weather

# COMMAND ----------

# GERAL
# print(weather.keys())
# print('')
# print(weather.values())

# COMMAND ----------

# DIA A DIA 
# print(weather['forecast'][1].keys())
# print('')
# print(weather['forecast'][1].values())

# COMMAND ----------

# PROTOTIPANDO ESTRUTURA DE DADOS GERAIS
# print([(weather['location']),(weather['region']),(weather['country']),(weather['latitude']),(weather['longitude']),(weather['timezone'])])

# COMMAND ----------

# PROTOTIPANDO ESTRUTURA DE DADOS DA PREVISÃO DIARIA
# print([(weather['forecast'][1]['date']),(weather['forecast'][1]['max_temp_c']),(weather['forecast'][1]['min_temp_c']),\
#       (weather['forecast'][1]['avg_temp_c']),(weather['forecast'][1]['will_it_rain']),(weather['forecast'][1]['chance_of_rain']),\
#       (weather['forecast'][1]['condition']),(weather['forecast'][1]['sunrise']),(weather['forecast'][1]['sunset']),\
#       (weather['forecast'][1]['max_wind_kph'])])

# COMMAND ----------

# MAGIC %md
# MAGIC ###### REQUISITANDO API

# COMMAND ----------

# VARIAVEIS PARA CONSTRUCAO DO DATAFRAME
previsao = []
previsao_schema = 'CIDADE STRING,            \
                   UF STRING,                \
                   PAIS STRING,              \
                   LAT STRING,               \
                   LONG STRING,              \
                   TIMEZONE STRING,          \
                   DATEVENTO STRING,         \
                   MAX_TEMP STRING,          \
                   MIN_TEMP STRING,          \
                   AVG_TEMP STRING,          \
                   CHUVA STRING,             \
                   CHANCE_CHUVA STRING,      \
                   CONDICAOTEMPO STRING,     \
                   NASCER_SOL STRING,        \
                   POR_SOL    STRING,        \
                   VELOCIDADE_VENTO STRING'


# COMMAND ----------

for i in range(qtde_cidades):
    # SELECIONANDO CADA CIDADE NO RANGE DA TABELA
    cidade = cidades.collect()[i][0]
    # REQUISITANDO API PARA A CIDADE
    weather = req.post("https://api.m3o.com/v1/weather/Forecast",
              json = {'days': days, 'location': cidade},
              headers = {"content-type" : content_type, "Authorization" : token})
    # PARAR CASO O REQUEST FALHAR
    if weather.status_code != 200:
        i = i+1
        cidade = cidades.collect()[i][0]
        weather = req.post("https://api.m3o.com/v1/weather/Forecast",
                  json = {'days': days, 'location': cidade},
                  headers = {"content-type" : content_type, "Authorization" : token})
    # ATRIBUINDO JSON A VARIAVEL
    weather = weather.json()
    # OBTENDO OS DADOS GERAIS DA LOCALIZACAO
    for d in range(days):
    # OBTENDO OS DADOS DE PREVISAO DA LOCALIZACAO
         previsao.append([(weather['location']),(weather['region']),(weather['country']),(weather['latitude']),(weather['longitude']),(weather['timezone']),\
                        (weather['forecast'][d]['date']),(weather['forecast'][d]['max_temp_c']),(weather['forecast'][d]['min_temp_c']),\
                        (weather['forecast'][d]['avg_temp_c']),(weather['forecast'][d]['will_it_rain']),(weather['forecast'][d]['chance_of_rain']),\
                        (weather['forecast'][d]['condition']),(weather['forecast'][d]['sunrise']),(weather['forecast'][d]['sunset']),\
                        (weather['forecast'][d]['max_wind_kph'])])

# COMMAND ----------

# CRIACAO DO DATAFRAME
previsao_es  = spark.createDataFrame(previsao, previsao_schema)

# COMMAND ----------

# CRIANDO UM TEMP VIEW "TEMPERATURA_ES"
previsao_es.createOrReplaceTempView("TEMPERATURA_ES")

# COMMAND ----------

# SELECT DA TABELA PARA VERIFICAR CRIAÇÃO
spark.sql("\
             SELECT TP.CIDADE,\
                    TP.DATEVENTO AS DATA,\
                    TP.MAX_TEMP AS MAXIMA,\
                    TP.MIN_TEMP AS MINIMA,\
                    TP.CONDICAOTEMPO\
               FROM TEMPERATURA_ES TP\
              WHERE TP.CIDADE = 'Vila Velha'\
          ").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETAPA 3  
# MAGIC CRIAR DATAFRAMES DAS TABELAS 1 E 2

# COMMAND ----------

# MAGIC %sql
# MAGIC --PROTOTIPO DA TABELA 1
# MAGIC   SELECT TES.CIDADE,
# MAGIC          C.ID AS CODIGODACIDADE,
# MAGIC          TES.DATEVENTO AS DATA,
# MAGIC          TES.UF AS REGIAO,
# MAGIC          TES.PAIS,
# MAGIC          TES.LAT AS LATITUDE,
# MAGIC          TES.LONG AS LONGITUDE,
# MAGIC          TES.MAX_TEMP AS TEMPERATURAMAXIMA,
# MAGIC          TES.MIN_TEMP AS TEMPERATURAMINIMA,
# MAGIC          TES.AVG_TEMP AS TEMPERATURAMEDIA,
# MAGIC          (CASE 
# MAGIC                WHEN TES.CHUVA = 'true' THEN 'SIM'
# MAGIC                ELSE 'NAO'
# MAGIC                END ) AS VAICHOVER,
# MAGIC          TES.CHANCE_CHUVA AS CHANCECHUVA,
# MAGIC          TES.CONDICAOTEMPO,
# MAGIC          TES.NASCER_SOL,
# MAGIC          TES.POR_SOL,
# MAGIC          TES.VELOCIDADE_VENTO
# MAGIC     FROM TEMPERATURA_ES TES
# MAGIC          LEFT JOIN CIDADES_ES C ON TES.CIDADE = C.CIDADE
# MAGIC          WHERE TES.UF = 'Espirito Santo'

# COMMAND ----------

# SELECT PARA O DATAFRAME TABELA 1
tabela1 = spark.sql("\
             SELECT TES.CIDADE,\
                    C.ID AS CODIGODACIDADE,\
                    TES.DATEVENTO AS DATA,\
                    TES.UF AS REGIAO,\
                    TES.PAIS,\
                    TES.LAT AS LATITUDE,\
                    TES.LONG AS LONGITUDE,\
                    TES.MAX_TEMP AS TEMPERATURAMAXIMA,\
                    TES.MIN_TEMP AS TEMPERATURAMINIMA,\
                    TES.AVG_TEMP AS TEMPERATURAMEDIA,\
                    (CASE \
                          WHEN TES.CHUVA = 'true' THEN 'SIM'\
                          ELSE 'NAO'\
                          END ) AS VAICHOVER,\
                    TES.CHANCE_CHUVA AS CHANCECHUVA,\
                    TES.CONDICAOTEMPO,\
                    TES.NASCER_SOL,\
                    TES.POR_SOL,\
                    TES.VELOCIDADE_VENTO\
               FROM TEMPERATURA_ES TES\
                    LEFT JOIN CIDADES_ES C ON TES.CIDADE = C.CIDADE\
                    WHERE TES.UF = 'Espirito Santo'\
                    ")

# COMMAND ----------

# MAGIC %sql
# MAGIC --PROTOTIPO DA TABELA 2
# MAGIC   WITH COMCHUVA AS (
# MAGIC        SELECT TES.CIDADE,
# MAGIC               COALESCE(COUNT(TES.CHUVA),0) AS QTDCOM
# MAGIC          FROM TEMPERATURA_ES TES
# MAGIC         WHERE TES.UF = 'Espirito Santo'
# MAGIC           AND TES.CHUVA = 'true'
# MAGIC         GROUP BY TES.CIDADE),
# MAGIC        SEMCHUVA AS (
# MAGIC        SELECT TES.CIDADE,
# MAGIC               COALESCE(COUNT(TES.CHUVA),0) AS QTDSEM
# MAGIC          FROM TEMPERATURA_ES TES
# MAGIC         WHERE TES.UF = 'Espirito Santo'
# MAGIC           AND TES.CHUVA = 'false'
# MAGIC         GROUP BY TES.CIDADE)
# MAGIC   
# MAGIC   SELECT TES.CIDADE,
# MAGIC          COM.QTDCOM AS QTDDIASVAICHOVER,
# MAGIC          SEM.QTDSEM AS QTDDIASNAOVAICHOVER,
# MAGIC          COUNT(TES.DATEVENTO) AS TOTALDIASMAPEADOS
# MAGIC     FROM TEMPERATURA_ES TES
# MAGIC          LEFT JOIN COMCHUVA COM ON TES.CIDADE = COM.CIDADE
# MAGIC          LEFT JOIN SEMCHUVA SEM ON TES.CIDADE = SEM.CIDADE
# MAGIC     WHERE TES.UF = 'Espirito Santo'
# MAGIC     GROUP BY TES.CIDADE,
# MAGIC              QTDDIASVAICHOVER,
# MAGIC              QTDDIASNAOVAICHOVER

# COMMAND ----------

# SELECT PARA O DATAFRAME TABELA 2
tabela2 = spark.sql("\
             WITH COMCHUVA AS (\
                   SELECT TES.CIDADE,\
                          COALESCE(COUNT(TES.CHUVA),0) AS QTDCOM\
                     FROM TEMPERATURA_ES TES\
                    WHERE TES.UF = 'Espirito Santo'\
                      AND TES.CHUVA = 'true'\
                    GROUP BY TES.CIDADE),\
                  SEMCHUVA AS (\
                   SELECT TES.CIDADE,\
                          COALESCE(COUNT(TES.CHUVA),0) AS QTDSEM\
                     FROM TEMPERATURA_ES TES\
                    WHERE TES.UF = 'Espirito Santo'\
                      AND TES.CHUVA = 'false'\
                    GROUP BY TES.CIDADE)\
  \
              SELECT TES.CIDADE,\
                     COM.QTDCOM AS QTDDIASVAICHOVER,\
                     SEM.QTDSEM AS QTDDIASNAOVAICHOVER,\
                     COUNT(TES.DATEVENTO) AS TOTALDIASMAPEADOS\
                FROM TEMPERATURA_ES TES\
                     LEFT JOIN COMCHUVA COM ON TES.CIDADE = COM.CIDADE\
                     LEFT JOIN SEMCHUVA SEM ON TES.CIDADE = SEM.CIDADE\
                WHERE TES.UF = 'Espirito Santo'\
                GROUP BY TES.CIDADE,\
                         QTDDIASVAICHOVER,\
                         QTDDIASNAOVAICHOVER\
                    ")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETAPA 4  
# MAGIC EXPORTAR CSV DOS DATAFRAMES DAS TABELAS 1 E 2

# COMMAND ----------

# CSV
tabela1.write.format('csv').save('/FileStore/tables/tabela1')
tabela2.write.format('csv').save('/FileStore/tables/tabela2')
