# Databricks notebook source
from pyspark.sql.functions import *
# lectura de tablas a synapse
import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import date_format

# ejecucion de script tipo update en la base de datos
import pyodbc

# trabajo con tablas delta
from delta.tables import *

# manejo logs
import logging

# funciones trabajo con json
from pyspark.sql import functions as F

# COMMAND ----------

download = ""

# COMMAND ----------

# Parametros
pathRaw = 'abfss://raw@datalakecomfama.dfs.core.windows.net/'
pathBronze = 'abfss://bronze@datalakecomfama.dfs.core.windows.net/'

pending1 = 'Empleo2.0/googleAnalytics/GA4_V1/pending/'
finish1 = 'Empleo2.0/googleAnalytics/GA4_V1/finish/'

pending2 = 'Empleo2.0/googleAnalytics/GA4_V2/pending/'
finish2 = 'Empleo2.0/googleAnalytics/GA4_V2/finish/'

pending3 = 'Empleo2.0/googleAnalytics/GA4_V3/pending/'
finish3 = 'Empleo2.0/googleAnalytics/GA4_V3/finish/'

pending4 = 'Empleo2.0/googleAnalytics/GA4_V4/pending/'
finish4 = 'Empleo2.0/googleAnalytics/GA4_V4/finish/'

pending5 = 'Empleo2.0/googleAnalytics/GA4_V5/pending/'
finish5 = 'Empleo2.0/googleAnalytics/GA4_V5/finish/'

pending6 = 'Empleo2.0/googleAnalytics/GA4_V6/pending/'
finish6 = 'Empleo2.0/googleAnalytics/GA4_V6/finish/'

pending7 = 'Empleo2.0/googleAnalytics/GA4_V7/pending/'
finish7 = 'Empleo2.0/googleAnalytics/GA4_V7/finish/'

server = 'synaps-comfama.database.windows.net'
#database = 'synapse_comfama'
database = 'synapse_comfama_dev'
ODBCDriver = 'ODBC Driver 17 for SQL Server'
#UserID = 'UsrAlgoritmoPrd'
#PassWD = TokenLibrary.getSecret('tecnologia', 'KV-UsrAlgoritmo-PRD', 'tecnologia')

UserID = 'UsrAlgoritmo'
PassWD = '4lg0r1tm0*'

auditTable = 'framework.AuditoriaEjecucionPipelineSQL'
configTable = 'framework.ConfiguracionAdfMetadataSQL'
auditTableAPI = 'framework.AuditoriaEjecucionPipelineAPI'
configTableAPI = 'framework.ConfiguracionAdfMetadataAPI'

tipoCargaTablaCompleta = '0'
tipoCargaTablaIncremental = '1'
tipoCargaTablaUpsert = '2'

schemaBronze = 'bronze'
schemaSilver = 'silver'
schemaGold = 'gold'
estadoCargaExitosa = 'Exitoso'
estadoCargaFallido = 'Fallido'

zonaActualizarBronce = 'estadoBronce'

slash = "/"

deleteDeltaTable = "DELETE FROM {}.{}"
refreshDeltaTable = "REFRESH TABLE {}.{}"
vacuumDeltaTable = "VACUUM {}.{} RETAIN 170 HOURS"
dropDeltaTable = "DROP TABLE IF EXISTS {}.{}"
createDeltaTable = "CREATE TABLE {}.{} USING DELTA LOCATION '{}'"
createDeltaTablePartition = "CREATE TABLE {}.{} USING DELTA LOCATION '{}' PARTITIONED BY ({})"

# COMMAND ----------

# MAGIC %%pyspark
# MAGIC def path_exists(path):
# MAGIC     try:
# MAGIC         mssparkutils.fs.ls(path)
# MAGIC         return True
# MAGIC     except Exception as e:
# MAGIC         return False

# COMMAND ----------

# MAGIC %%pyspark
# MAGIC def file_pending(path):
# MAGIC     folderPath = mssparkutils.fs.ls(path)
# MAGIC     for file in folderPath:
# MAGIC         pendingFile = file.name
# MAGIC     return pendingFile

# COMMAND ----------

# MAGIC %%pyspark
# MAGIC def createFullDeltaTable(newpath: str, esquema: str, nombreTabla: str, dfNew):
# MAGIC     try:
# MAGIC         if path_exists(newpath):
# MAGIC             mssparkutils.fs.rm(newpath, True)
# MAGIC         
# MAGIC         spark.sql(dropDeltaTable.format(esquema, nombreTabla))
# MAGIC         dfNew.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(newpath)
# MAGIC         spark.sql(createDeltaTable.format(esquema, nombreTabla, newpath))
# MAGIC 
# MAGIC         return True
# MAGIC     except Exception as e:
# MAGIC         print("Error creando la tabla: "+nombreTabla+" debido a: "+str(e))
# MAGIC         return False

# COMMAND ----------

def chooseVersion(download):

   if download == "1":
      pathPending = pending1
      finalBronze = pathPending[:len(pathPending) - len('/pending/')]
      pathFinish = finish1
      tablaBronze = 'googleanltcs_MetricasGA4V1'
   elif download == "2":
      finalBronze = 'Empleo2.0/googleAnalytics/GA4_V2/'
      pathPending = pending2
      pathFinish = finish2
      tablaBronze = 'googleanltcs_MetricasGA4V2'
   elif download == "3":
      finalBronze = 'Empleo2.0/googleAnalytics/GA4_V3/'
      pathPending = pending3
      pathFinish = finish3
      tablaBronze = 'googleanltcs_MetricasGA4V3'
   elif download == "4":
      finalBronze = 'Empleo2.0/googleAnalytics/GA4_V4/'
      pathPending = pending4
      pathFinish = finish4
      tablaBronze = 'googleanltcs_MetricasGA4V4'
   elif download == "5":
      finalBronze = 'Empleo2.0/googleAnalytics/GA4_V5/'
      pathPending = pending5
      pathFinish = finish5
      tablaBronze = 'googleanltcs_MetricasGA4V5'
   elif download == "6":
      finalBronze = 'Empleo2.0/googleAnalytics/GA4_V6/'
      pathPending = pending6
      pathFinish = finish6
      tablaBronze = 'googleanltcs_MetricasGA4V6'
   elif download == "7":
      finalBronze = 'Empleo2.0/googleAnalytics/GA4_V7/'
      pathPending = pending7
      pathFinish = finish7
      tablaBronze = 'googleanltcs_MetricasGA4V7'

   return finalBronze, pathPending, pathFinish, tablaBronze

# COMMAND ----------

def readJson(oldPath: str):
    try:
        dfNew = spark.read.option("multiline","true").option("inferSchema", True).option("header", True).json(oldPath)
        return dfNew
    except Exception as e:
        print("Error leyendo json de la ruta: "+oldPath+" debido a: "+str(e))
        return False

# COMMAND ----------

def create_df(df):
    # las columnas "dimensionHeaders" y "metricHeaders" contienen el nombre de las columnas del dataframe que queremos obtener
    columns1=df.select("dimensionHeaders").first()[0]
    columns2=df.select("metricHeaders").first()[0]
    #se une la informacion de estas dos columnas y el resultado final sera una lista con el nombre de las columnas del dataframe final
    column_names=[x[0] for x in columns1]+[col[0] for col in columns2]
    
    # la columna "rows" de df contiene lo que serian las filas de cada columna del dataframe final
    values = df.select("rows").first()[0] 
    #creamos una lista que contendra los valores de esas filas con el formato deseado
    final_values=[]
    for val in values:
        joined_value =[x for x in val[0]]+[x for x in val[1]]
        value_to_agregate=[value[0] for value in joined_value]
        final_values.append(value_to_agregate)
    
    #finalmente se crea el dataframe al cual se le tendran que agregar otras 2 columnas
    df = spark.createDataFrame(final_values, column_names)
    df=df.withColumn('InsertDate', F.current_timestamp())

    return df

# COMMAND ----------

# MAGIC %%pyspark
# MAGIC try:
# MAGIC     
# MAGIC     finalBronze, pending, finish, tablaBronze = chooseVersion(download)
# MAGIC 
# MAGIC     #Step1: llamar la funcion readJson
# MAGIC     df=readJson(pathRaw + pending)
# MAGIC     #Step2: llamar la funcion create_df
# MAGIC     df_final=create_df(df)
# MAGIC 
# MAGIC     if DeltaTable.isDeltaTable(spark, pathBronze + finalBronze):
# MAGIC         print("Existe delta table")
# MAGIC         df_final.write.format("delta").mode("append").save(pathBronze + finalBronze)
# MAGIC 
# MAGIC     else:
# MAGIC         print("no existe la tabla delta")
# MAGIC         createFullDeltaTable(pathBronze + finalBronze, schemaBronze, tablaBronze, df_final)
# MAGIC     
# MAGIC     print('Vamos a mover el archivo procesado')
# MAGIC     fileNameFinish = file_pending(pathRaw + pending)
# MAGIC     mssparkutils.fs.mv(pathRaw + pending + fileNameFinish, pathRaw + finish, True) 
# MAGIC 
# MAGIC 
# MAGIC except Exception as e:
# MAGIC     print("Error procesando los datos del día debido a: "+str(e))
