# Databricks notebook source
# DBTITLE 1,Libraries
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.ml.feature import Imputer
import datetime as dt
from datetime import date
from delta.tables import *
import pandas

# COMMAND ----------

dbutils.widgets.remove('Environment_Variable'

# COMMAND ----------

# DBTITLE 1,Environment
#dbutils.widgets.dropdown('Environment_Variable', 'dev', ['dev', 'prod'])
environment_var = dbutils.widgets.get("Environment_Variable")

#def var_environment() :
#  environment = environment_var
#  return environment

#print(environment_var)

# COMMAND ----------

# DBTITLE 1,Tables & Zones
#-------------------Zonas 

# bronce stage
enviroment_des_bronce = "bronce_mx_poc_rotacion"
enviroment_pro_bronce = ""

# silver stage
enviroment_des_silver = "silver_mx_DriversRotacion.silver_dev_"
enviroment_pro_silver = "silver_mx_DriversRotacion.silver_pro_"

# gold stage
enviroment_des_gold = "gold_mx_DriversRotacion.gold_dev_"
enviroment_pro_gold = "gold_mx_DriversRotacion.gold_pro_"

# Temporal Stage
enviroment_des_temp = "temporal_rotacion"
enviroment_pro_temp = ""

#-------------------Gold Tablas 
imputed_data_glob = "imputed_data_glob"
mx_dataset01_walmex_completo = "mx_dataset01_walmex_completo"
grupos_puesto = "grupos_puesto"
mx_cat_puestos = "mx_cat_puestos"
unidades_en_operaci_n_19_01_1_csv = "unidades_en_operaci_n_19_01_1_csv"
mx_dataset03_walmex_csv = "mx_dataset03_walmex_csv"
mx_dataset06_walmex_csv ="mx_dataset06_walmex_csv"
tabuladores_2022 = "tabuladores_2022"
tabuladores_2019_2021 = "tabuladores_2019_2021"
mx_dataset02_walmex_csv = "mx_dataset02_walmex_csv"
mx_dataset05_walmex_csv = "mx_dataset05_walmex_csv"
historico_bono_2019_2021 = "historico_bono_2019_2021"
dar_mar_oct2021 = "dar_mar_oct2021"
dl_calendar_dim = "dl_calendar_dim"
data_ru = "data_ru"
mx_general_inf_associate = "mx_general_inf_associate"

#------------------- Schemas ----------------------#
schema_bronce = "bronce_mx_DriversRotacion"
schema_silver = "silver_mx_DriversRotacion"
schema_gold = "gold_mx_DriversRotacion"

#--------------------Silver Tables Dev ------------#
silver_dev_tb_PerfEval             = "silver_dev_mx_PerfEval"
silver_dev_tb_EmpJob               = "silver_dev_mx_EmpJob"
silver_dev_tb_SfLocation           = "silver_dev_mx_SfLocation"
silver_dev_tb_DimPersonal          = "silver_dev_mx_DimPersonal"
silver_dev_tb_DimEmppay            = "silver_dev_mx_DimEmppay"
silver_dev_tb_DimEmployment        = "silver_dev_mx_DimEmployment"
silver_dev_tb_SfStore              = "silver_dev_mx_SfStore"
silver_dev_tb_TimeAccount          = "silver_dev_mx_TimeAccount"
silver_dev_tb_TimeAccountDtl       = "silver_dev_mx_TimeAccountDtl"
silver_dev_tb_Dependents           = "silver_dev_mx_Dependents"
silver_dev_tb_DisciplinaryMeasures = "silver_dev_mx_DisciplinaryMeasures"
silver_dev_tb_SfJobCode            = "silver_dev_mx_SfJobCode"
silver_dev_tb_CNC                  = "silver_dev_mx_CNC"
silver_dev_tb_DEL                  = "silver_dev_mx_DEL"
silver_dev_tb_DAR                  = "silver_dev_mx_DAR"
silver_dev_tb_Position             = "silver_dev_mx_Position"
silver_dev_tb_Tabulator            = "silver_dev_mx_Tabulator"
silver_dev_tb_PayRoll              = "silver_dev_mx_PayRoll"
silver_dev_tb_ChristmasBox         = "silver_dev_mx_ChristmasBox"
silver_dev_tb_AnnualBonus          = "silver_dev_mx_AnnualBonus"
silver_dev_tb_Training             = "silver_dev_mx_Training"
silver_dev_tb_MipHistorico         = "silver_dev_mx_MipHistorico"
silver_dev_tb_catEventReason       = "silver_dev_mx_catEventReason"

#--------------------Silver Tables Prod ------------#
silver_prod_tb_PerfEval             = "silver_prod_mx_PerfEval"
silver_prod_tb_EmpJob               = "silver_prod_mx_EmpJob"
silver_prod_tb_SfLocation           = "silver_prod_mx_SfLocation"
silver_prod_tb_DimPersonal          = "silver_prod_mx_DimPersonal"
silver_prod_tb_DimEmppay            = "silver_prod_mx_DimEmppay"
silver_prod_tb_DimEmployment        = "silver_prod_mx_DimEmployment"
silver_prod_tb_SfStore              = "silver_prod_mx_SfStore"
silver_prod_tb_TimeAccount          = "silver_prod_mx_TimeAccount"
silver_prod_tb_TimeAccountDtl       = "silver_prod_mx_TimeAccountDtl"
silver_prod_tb_Dependents           = "silver_prod_mx_Dependents"
silver_prod_tb_DisciplinaryMeasures = "silver_prod_mx_DisciplinaryMeasures"
silver_prod_tb_SfJobCode            = "silver_prod_mx_SfJobCode"
silver_prod_tb_CNC                  = "silver_prod_mx_CNC"
silver_prod_tb_DEL                  = "silver_prod_mx_DEL"
silver_prod_tb_DAR                  = "silver_prod_mx_DAR"
silver_prod_tb_Position             = "silver_prod_mx_Position"
silver_prod_tb_Tabulator            = "silver_prod_mx_Tabulator"
silver_prod_tb_PayRoll              = "silver_prod_mx_PayRoll"
silver_prod_tb_ChristmasBox         = "silver_prod_mx_ChristmasBox"
silver_prod_tb_AnnualBonus          = "silver_prod_mx_AnnualBonus"
silver_prod_tb_Training             = "silver_prod_mx_Training"
silver_prod_tb_MipHistorico         = "silver_prod_mx_MipHistorico"
silver_prod_tb_catEventReason       = "silver_prod_mx_catEventReason"

#--------------------gold Tables Dev ------------#
gold_dev_tb_PerfEval             = "gold_dev_mx_PerfEval"
gold_dev_tb_EmpJob               = "gold_dev_mx_EmpJob"
gold_dev_tb_SfLocation           = "gold_dev_mx_SfLocation"
gold_dev_tb_DimPersonal          = "gold_dev_mx_DimPersonal"
gold_dev_tb_DimEmppay            = "gold_dev_mx_DimEmppay"
gold_dev_tb_DimEmployment        = "gold_dev_mx_DimEmployment"
gold_dev_tb_SfStore              = "gold_dev_mx_SfStore"
gold_dev_tb_TimeAccount          = "gold_dev_mx_TimeAccount"
gold_dev_tb_TimeAccountDtl       = "gold_dev_mx_TimeAccountDtl"
gold_dev_tb_Dependents           = "gold_dev_mx_Dependents"
gold_dev_tb_DisciplinaryMeasures = "gold_dev_mx_DisciplinaryMeasures"
gold_dev_tb_SfJobCode            = "gold_dev_mx_SfJobCode"
gold_dev_tb_CNC                  = "gold_dev_mx_CNC"
gold_dev_tb_DEL                  = "gold_dev_mx_DEL"
gold_dev_tb_DAR                  = "gold_dev_mx_DAR"
gold_dev_tb_Position             = "gold_dev_mx_Position"
gold_dev_tb_Tabulator            = "gold_dev_mx_Tabulator"
gold_dev_tb_PayRoll              = "gold_dev_mx_PayRoll"
gold_dev_tb_ChristmasBox         = "gold_dev_mx_ChristmasBox"
gold_dev_tb_AnnualBonus          = "gold_dev_mx_AnnualBonus"
gold_dev_tb_Training             = "gold_dev_mx_Training"
gold_dev_tb_MipHistorico         = "gold_dev_mx_MipHistorico"

#--------------------gold Tables Prod ------------#
gold_prod_tb_PerfEval             = "gold_prod_mx_PerfEval"
gold_prod_tb_EmpJob               = "gold_prod_mx_EmpJob"
gold_prod_tb_SfLocation           = "gold_prod_mx_SfLocation"
gold_prod_tb_DimPersonal          = "gold_prod_mx_DimPersonal"
gold_prod_tb_DimEmppay            = "gold_prod_mx_DimEmppay"
gold_prod_tb_DimEmployment        = "gold_prod_mx_DimEmployment"
gold_prod_tb_SfStore              = "gold_prod_mx_SfStore"
gold_prod_tb_TimeAccount          = "gold_prod_mx_TimeAccount"
gold_prod_tb_TimeAccountDtl       = "gold_prod_mx_TimeAccountDtl"
gold_prod_tb_Dependents           = "gold_prod_mx_Dependents"
gold_prod_tb_DisciplinaryMeasures = "gold_prod_mx_DisciplinaryMeasures"
gold_prod_tb_SfJobCode            = "gold_prod_mx_SfJobCode"
gold_prod_tb_CNC                  = "gold_prod_mx_CNC"
gold_prod_tb_DEL                  = "gold_prod_mx_DEL"
gold_prod_tb_DAR                  = "gold_prod_mx_DAR"
gold_prod_tb_Position             = "gold_prod_mx_Position"
gold_prod_tb_Tabulator            = "gold_prod_mx_Tabulator"
gold_prod_tb_PayRoll              = "gold_prod_mx_PayRoll"
gold_prod_tb_ChristmasBox         = "gold_prod_mx_ChristmasBox"
gold_prod_tb_AnnualBonus          = "gold_prod_mx_AnnualBonus"
gold_prod_tb_Training             = "gold_prod_mx_Training"
gold_prod_tb_MipHistorico         = "gold_prod_mx_MipHistorico"

mx_EmpJob = "mx_EmpJob"
mx_DimEmppay = "mx_DimEmppay"
mx_DimPersonal = "mx_DimPersonal"
mx_DimEmployment = "mx_DimEmployment"
mx_SfStore = "mx_SfStore"
mx_SfLocation = "mx_SfLocation"
mx_PerfEval = "mx_PerfEval"
mx_catEventReason = "mx_catEventReason"
mx_Position = "mx_Position"
#--------------------Bronze Tables

password = erd5467

# COMMAND ----------

    
def path_read(ambiente,zona,tabla):
  
  if ambiente == "dev" and zona == "bronce" :
    base = enviroment_des_bronce
    
  if ambiente == "pro" and zona == "bronce" :
    base = enviroment_pro_bronce
    
  if ambiente == "dev" and zona == "silver" :
    base = enviroment_des_silver
    
  if ambiente == "pro" and zona == "silver" :
    base = enviroment_pro_silver

  if ambiente == "dev" and zona == "temp" :
    base = enviroment_des_temp
    
  if ambiente == "pro" and zona == "temp" :
    base = enviroment_pro_temp
  
  # validacion de tablas   
  if tabla == imputed_data_glob :
    tablaf = imputed_data_glob
  if tabla == mx_dataset01_walmex_completo :
    tablaf = mx_dataset01_walmex_completo
  if tabla == grupos_puesto :
    tablaf = grupos_puesto
  if tabla == mx_cat_puestos :
    tablaf = mx_cat_puestos
  if tabla == unidades_en_operaci_n_19_01_1_csv :
    tablaf = unidades_en_operaci_n_19_01_1_csv
  if tabla == mx_dataset03_walmex_csv :
    tablaf = mx_dataset03_walmex_csv
  if tabla == mx_dataset06_walmex_csv :
    tablaf = mx_dataset06_walmex_csv
  if tabla == tabuladores_2022 :
    tablaf = tabuladores_2022
  if tabla == tabuladores_2019_2021 :
    tablaf = tabuladores_2019_2021
  if tabla == mx_dataset02_walmex_csv :
    tablaf = mx_dataset02_walmex_csv
  if tabla == mx_dataset05_walmex_csv :
    tablaf = mx_dataset05_walmex_csv
  if tabla == historico_bono_2019_2021 :
    tablaf = historico_bono_2019_2021
  if tabla == dar_mar_oct2021 :
    tablaf = dar_mar_oct2021
  if tabla == dl_calendar_dim :
    tablaf = dl_calendar_dim
  if tabla == data_ru :
    tablaf = data_ru
    
  if tabla == "mx_EmpJob" :
    tablaf = mx_EmpJob
  if tabla == "mx_DimEmppay" :
    tablaf = mx_DimEmppay
  if tabla == "mx_DimPersonal" :
    tablaf = mx_DimPersonal
  if tabla == "mx_DimEmployment" : 
    tablaf = mx_DimEmployment
  if tabla == "mx_SfStore" :
    tablaf = mx_SfStore
  if tabla == "mx_SfLocation" :
    tablaf = mx_SfLocation
  if tabla == "mx_PerfEval" :
    tablaf = mx_PerfEval
  if tabla == "mx_catEventReason":
    tablaf = mx_catEventReason
  
  if tabla == "mx_Position":
    tablaf = mx_Position
    
        
  return spark.table(base  + tablaf)


# COMMAND ----------

def path_write(ambiente,zona,tabla,df):
  
  if ambiente == "dev" and zona == "bronce" :
    base = enviroment_des_bronce
    
  if ambiente == "pro" and zona == "bronce" :
    base = enviroment_pro_bronce
    
  if ambiente == "dev" and zona == "silver" :
    base = enviroment_des_silver
    
  if ambiente == "pro" and zona == "silver" :
    base = enviroment_pro_silver
    
  if ambiente == "dev" and zona == "gold" :
    base = enviroment_des_gold
    
  if ambiente == "pro" and zona == "gold" :
    base = enviroment_pro_gold

  if ambiente == "dev" and zona == "temp" :
    base = enviroment_des_temp
    
  if ambiente == "pro" and zona == "temp" :
    base = enviroment_pro_temp
  
  # validacion de tablas   
  if tabla == imputed_data_glob :
    tablaf = imputed_data_glob
  if tabla == mx_dataset01_walmex_completo :
    tablaf = mx_dataset01_walmex_completo
  if tabla == grupos_puesto :
    tablaf = grupos_puesto
  if tabla == mx_cat_puestos :
    tablaf = mx_cat_puestos
  if tabla == unidades_en_operaci_n_19_01_1_csv :
    tablaf = unidades_en_operaci_n_19_01_1_csv
  if tabla == mx_dataset03_walmex_csv :
    tablaf = mx_dataset03_walmex_csv
  if tabla == mx_dataset06_walmex_csv :
    tablaf = mx_dataset06_walmex_csv
  if tabla == tabuladores_2022 :
    tablaf = tabuladores_2022
  if tabla == tabuladores_2019_2021 :
    tablaf = tabuladores_2019_2021
  if tabla == mx_dataset02_walmex_csv :
    tablaf = mx_dataset02_walmex_csv
  if tabla == mx_dataset05_walmex_csv :
    tablaf = mx_dataset05_walmex_csv
  if tabla == historico_bono_2019_2021 :
    tablaf = historico_bono_2019_2021
  if tabla == dar_mar_oct2021 :
    tablaf = dar_mar_oct2021
  if tabla == dl_calendar_dim :
    tablaf = dl_calendar_dim
  if tabla == data_ru :
    tablaf = data_ru
    
  if tabla == "mx_EmpJob" :
    tablaf = mx_EmpJob
  if tabla == "mx_DimEmppay" :
    tablaf = mx_DimEmppay
  if tabla == "mx_DimPersonal" :
    tablaf = mx_DimPersonal
  if tabla == "mx_DimEmployment" : 
    tablaf = mx_DimEmployment
  if tabla == "mx_SfStore" :
    tablaf = mx_SfStore
  if tabla == "mx_SfLocation" :
    tablaf = mx_SfLocation
  if tabla == "mx_general_inf_associate" :
    tablaf = mx_general_inf_associate
  if tabla == "mx_PerfEval" :
    tablaf = mx_PerfEval
        
  return df.write.format("delta").mode("overwrite").saveAsTable(base  + tablaf)

# COMMAND ----------

def make_path (ambiente,zona,tabla):
  
  if ambiente == "dev" and zona == "bronce" :
    base = enviroment_des_bronce
    
  if ambiente == "pro" and zona == "bronce" :
    base = enviroment_pro_bronce
    
  if ambiente == "dev" and zona == "silver" :
    base = enviroment_des_silver
    
  if ambiente == "pro" and zona == "silver" :
    base = enviroment_pro_silver

  if ambiente == "dev" and zona == "temp" :
    base = enviroment_des_temp
    
  if ambiente == "pro" and zona == "temp" :
    base = enviroment_pro_temp
  
  # validacion de tablas   
  if tabla == imputed_data_glob :
    tablaf = imputed_data_glob
  if tabla == mx_dataset01_walmex_completo :
    tablaf = mx_dataset01_walmex_completo
  if tabla == grupos_puesto :
    tablaf = grupos_puesto
  if tabla == mx_cat_puestos :
    tablaf = mx_cat_puestos
  if tabla == unidades_en_operaci_n_19_01_1_csv :
    tablaf = unidades_en_operaci_n_19_01_1_csv
  if tabla == mx_dataset03_walmex_csv :
    tablaf = mx_dataset03_walmex_csv
  if tabla == mx_dataset06_walmex_csv :
    tablaf = mx_dataset06_walmex_csv
  if tabla == tabuladores_2022 :
    tablaf = tabuladores_2022
  if tabla == tabuladores_2019_2021 :
    tablaf = tabuladores_2019_2021
  if tabla == mx_dataset02_walmex_csv :
    tablaf = mx_dataset02_walmex_csv
  if tabla == mx_dataset05_walmex_csv :
    tablaf = mx_dataset05_walmex_csv
  if tabla == historico_bono_2019_2021 :
    tablaf = historico_bono_2019_2021
  if tabla == dar_mar_oct2021 :
    tablaf = dar_mar_oct2021
  if tabla == dl_calendar_dim :
    tablaf = dl_calendar_dim
  if tabla == data_ru :
    tablaf = data_ru
    
  if tabla == "mx_EmpJob" :
    tablaf = mx_EmpJob
  if tabla == "mx_DimEmppay" :
    tablaf = mx_DimEmppay
  if tabla == "mx_DimPersonal" :
    tablaf = mx_DimPersonal
  if tabla == "mx_DimEmployment" : 
    tablaf = mx_DimEmployment
  if tabla == "mx_SfStore" :
    tablaf = mx_SfStore
  if tabla == "mx_SfLocation" :
    tablaf = mx_SfLocation    
  if tabla == "mx_PerfEval" :
    tablaf = mx_PerfEval
    
  return str(base)  + str(tablaf)

# COMMAND ----------

# DBTITLE 1,Define Schema Zona
def schema_zone (zona):
  
  if zona == "silver" :
      schema = schema_silver
  if zona == "gold" :
      schema = schema_gold
    
  return str(schema)

# COMMAND ----------

# DBTITLE 1,Define Tabla
def table_zone (ambiente,zona,table):
  
  #------------------------Tablas Silver Dev --------------------------#
  
  if ambiente == "dev" and zona == "silver" and table == "PerfEval":
      table_env_zone_val = silver_dev_tb_PerfEval
  if ambiente == "dev" and zona == "silver" and table == "EmpJob":
      table_env_zone_val = silver_dev_tb_EmpJob
  if ambiente == "dev" and zona == "silver" and table == "SfLocation":
      table_env_zone_val = silver_dev_tb_SfLocation
  if ambiente == "dev" and zona == "silver" and table == "DimPersonal":
      table_env_zone_val = silver_dev_tb_DimPersonal
  if ambiente == "dev" and zona == "silver" and table == "DimEmppay":
      table_env_zone_val = silver_dev_tb_DimEmppay
  if ambiente == "dev" and zona == "silver" and table == "DimEmployment":
      table_env_zone_val = silver_dev_tb_DimEmployment
  if ambiente == "dev" and zona == "silver" and table == "SfStore":
      table_env_zone_val = silver_dev_tb_SfStore
  if ambiente == "dev" and zona == "silver" and table == "TimeAccount":
      table_env_zone_val = silver_dev_tb_TimeAccount
  if ambiente == "dev" and zona == "silver" and table == "TimeAccountDtl":
      table_env_zone_val = silver_dev_tb_TimeAccountDtl
  if ambiente == "dev" and zona == "silver" and table == "Dependents":
      table_env_zone_val = silver_dev_tb_Dependents    
  if ambiente == "dev" and zona == "silver" and table == "DisciplinaryMeasures":
      table_env_zone_val = silver_dev_tb_DisciplinaryMeasures
  if ambiente == "dev" and zona == "silver" and table == "SfJobCode":
      table_env_zone_val = silver_dev_tb_SfJobCode
  if ambiente == "dev" and zona == "silver" and table == "CNC":
      table_env_zone_val = silver_dev_tb_CNC
  if ambiente == "dev" and zona == "silver" and table == "DEL":
      table_env_zone_val = silver_dev_tb_DEL
  if ambiente == "dev" and zona == "silver" and table == "DAR":
      table_env_zone_val = silver_dev_tb_DAR
  if ambiente == "dev" and zona == "silver" and table == "Position":
      table_env_zone_val = silver_dev_tb_Position
  if ambiente == "dev" and zona == "silver" and table == "Tabulator":
      table_env_zone_val = silver_dev_tb_Tabulator
  if ambiente == "dev" and zona == "silver" and table == "PayRoll":
      table_env_zone_val = silver_dev_tb_PayRoll
  if ambiente == "dev" and zona == "silver" and table == "ChristmasBox":
      table_env_zone_val = silver_dev_tb_ChristmasBox
  if ambiente == "dev" and zona == "silver" and table == "AnnualBonus":
      table_env_zone_val = silver_dev_tb_AnnualBonus  
  if ambiente == "dev" and zona == "silver" and table == "Training":
      table_env_zone_val = silver_dev_tb_Training
  if ambiente == "dev" and zona == "silver" and table == "MipHistorico":
      table_env_zone_val = silver_dev_tb_MipHistorico  

  #------------------------Tablas gold Dev --------------------------#
  
  if ambiente == "dev" and zona == "gold" and table == "PerfEval":
      table_env_zone_val = gold_dev_tb_PerfEval
  if ambiente == "dev" and zona == "gold" and table == "EmpJob":
      table_env_zone_val = gold_dev_tb_EmpJob
  if ambiente == "dev" and zona == "gold" and table == "SfLocation":
      table_env_zone_val = gold_dev_tb_SfLocation
  if ambiente == "dev" and zona == "gold" and table == "DimPersonal":
      table_env_zone_val = gold_dev_tb_DimPersonal
  if ambiente == "dev" and zona == "gold" and table == "DimEmppay":
      table_env_zone_val = gold_dev_tb_DimEmppay
  if ambiente == "dev" and zona == "gold" and table == "DimEmployment":
      table_env_zone_val = gold_dev_tb_DimEmployment
  if ambiente == "dev" and zona == "gold" and table == "SfStore":
      table_env_zone_val = gold_dev_tb_SfStore
  if ambiente == "dev" and zona == "gold" and table == "TimeAccount":
      table_env_zone_val = gold_dev_tb_TimeAccount
  if ambiente == "dev" and zona == "gold" and table == "TimeAccountDtl":
      table_env_zone_val = gold_dev_tb_TimeAccountDtl
  if ambiente == "dev" and zona == "gold" and table == "Dependents":
      table_env_zone_val = gold_dev_tb_Dependents    
  if ambiente == "dev" and zona == "gold" and table == "DisciplinaryMeasures":
      table_env_zone_val = gold_dev_tb_DisciplinaryMeasures
  if ambiente == "dev" and zona == "gold" and table == "SfJobCode":
      table_env_zone_val = gold_dev_tb_SfJobCode
  if ambiente == "dev" and zona == "gold" and table == "CNC":
      table_env_zone_val = gold_dev_tb_CNC
  if ambiente == "dev" and zona == "gold" and table == "DEL":
      table_env_zone_val = gold_dev_tb_DEL
  if ambiente == "dev" and zona == "gold" and table == "DAR":
      table_env_zone_val = gold_dev_tb_DAR
  if ambiente == "dev" and zona == "gold" and table == "Position":
      table_env_zone_val = gold_dev_tb_Position
  if ambiente == "dev" and zona == "gold" and table == "Tabulator":
      table_env_zone_val = gold_dev_tb_Tabulator
  if ambiente == "dev" and zona == "gold" and table == "PayRoll":
      table_env_zone_val = gold_dev_tb_PayRoll
  if ambiente == "dev" and zona == "gold" and table == "ChristmasBox":
      table_env_zone_val = gold_dev_tb_ChristmasBox
  if ambiente == "dev" and zona == "gold" and table == "AnnualBonus":
      table_env_zone_val = gold_dev_tb_AnnualBonus  
  if ambiente == "dev" and zona == "gold" and table == "Training":
      table_env_zone_val = gold_dev_tb_Training
  if ambiente == "dev" and zona == "gold" and table == "MipHistorico":
      table_env_zone_val = gold_dev_tb_MipHistorico  

  
  #------------------------Tablas Silver prod --------------------------#
  
  if ambiente == "prod" and zona == "silver" and table == "PerfEval":
      table_env_zone_val = silver_prod_tb_PerfEval
  if ambiente == "prod" and zona == "silver" and table == "EmpJob":
      table_env_zone_val = silver_prod_tb_EmpJob
  if ambiente == "prod" and zona == "silver" and table == "SfLocation":
      table_env_zone_val = silver_prod_tb_SfLocation
  if ambiente == "prod" and zona == "silver" and table == "DimPersonal":
      table_env_zone_val = silver_prod_tb_DimPersonal
  if ambiente == "prod" and zona == "silver" and table == "DimEmppay":
      table_env_zone_val = silver_prod_tb_DimEmppay
  if ambiente == "prod" and zona == "silver" and table == "DimEmployment":
      table_env_zone_val = silver_prod_tb_DimEmployment
  if ambiente == "prod" and zona == "silver" and table == "SfStore":
      table_env_zone_val = silver_prod_tb_SfStore
  if ambiente == "prod" and zona == "silver" and table == "TimeAccount":
      table_env_zone_val = silver_prod_tb_TimeAccount
  if ambiente == "prod" and zona == "silver" and table == "TimeAccountDtl":
      table_env_zone_val = silver_prod_tb_TimeAccountDtl
  if ambiente == "prod" and zona == "silver" and table == "Dependents":
      table_env_zone_val = silver_prod_tb_Dependents    
  if ambiente == "prod" and zona == "silver" and table == "DisciplinaryMeasures":
      table_env_zone_val = silver_prod_tb_DisciplinaryMeasures
  if ambiente == "prod" and zona == "silver" and table == "SfJobCode":
      table_env_zone_val = silver_prod_tb_SfJobCode
  if ambiente == "prod" and zona == "silver" and table == "CNC":
      table_env_zone_val = silver_prod_tb_CNC
  if ambiente == "prod" and zona == "silver" and table == "DEL":
      table_env_zone_val = silver_prod_tb_DEL
  if ambiente == "prod" and zona == "silver" and table == "DAR":
      table_env_zone_val = silver_prod_tb_DAR
  if ambiente == "prod" and zona == "silver" and table == "Position":
      table_env_zone_val = silver_prod_tb_Position
  if ambiente == "prod" and zona == "silver" and table == "Tabulator":
      table_env_zone_val = silver_prod_tb_Tabulator
  if ambiente == "prod" and zona == "silver" and table == "PayRoll":
      table_env_zone_val = silver_prod_tb_PayRoll
  if ambiente == "prod" and zona == "silver" and table == "ChristmasBox":
      table_env_zone_val = silver_prod_tb_ChristmasBox
  if ambiente == "prod" and zona == "silver" and table == "AnnualBonus":
      table_env_zone_val = silver_prod_tb_AnnualBonus  
  if ambiente == "prod" and zona == "silver" and table == "Training":
      table_env_zone_val = silver_prod_tb_Training
  if ambiente == "prod" and zona == "silver" and table == "MipHistorico":
      table_env_zone_val = silver_prod_tb_MipHistorico  

  #------------------------Tablas gold prod --------------------------#
  
  if ambiente == "prod" and zona == "gold" and table == "PerfEval":
      table_env_zone_val = gold_prod_tb_PerfEval
  if ambiente == "prod" and zona == "gold" and table == "EmpJob":
      table_env_zone_val = gold_prod_tb_EmpJob
  if ambiente == "prod" and zona == "gold" and table == "SfLocation":
      table_env_zone_val = gold_prod_tb_SfLocation
  if ambiente == "prod" and zona == "gold" and table == "DimPersonal":
      table_env_zone_val = gold_prod_tb_DimPersonal
  if ambiente == "prod" and zona == "gold" and table == "DimEmppay":
      table_env_zone_val = gold_prod_tb_DimEmppay
  if ambiente == "prod" and zona == "gold" and table == "DimEmployment":
      table_env_zone_val = gold_prod_tb_DimEmployment
  if ambiente == "prod" and zona == "gold" and table == "SfStore":
      table_env_zone_val = gold_prod_tb_SfStore
  if ambiente == "prod" and zona == "gold" and table == "TimeAccount":
      table_env_zone_val = gold_prod_tb_TimeAccount
  if ambiente == "prod" and zona == "gold" and table == "TimeAccountDtl":
      table_env_zone_val = gold_prod_tb_TimeAccountDtl
  if ambiente == "prod" and zona == "gold" and table == "Dependents":
      table_env_zone_val = gold_prod_tb_Dependents    
  if ambiente == "prod" and zona == "gold" and table == "DisciplinaryMeasures":
      table_env_zone_val = gold_prod_tb_DisciplinaryMeasures
  if ambiente == "prod" and zona == "gold" and table == "SfJobCode":
      table_env_zone_val = gold_prod_tb_SfJobCode
  if ambiente == "prod" and zona == "gold" and table == "CNC":
      table_env_zone_val = gold_prod_tb_CNC
  if ambiente == "prod" and zona == "gold" and table == "DEL":
      table_env_zone_val = gold_prod_tb_DEL
  if ambiente == "prod" and zona == "gold" and table == "DAR":
      table_env_zone_val = gold_prod_tb_DAR
  if ambiente == "prod" and zona == "gold" and table == "Position":
      table_env_zone_val = gold_prod_tb_Position
  if ambiente == "prod" and zona == "gold" and table == "Tabulator":
      table_env_zone_val = gold_prod_tb_Tabulator
  if ambiente == "prod" and zona == "gold" and table == "PayRoll":
      table_env_zone_val = gold_prod_tb_PayRoll
  if ambiente == "prod" and zona == "gold" and table == "ChristmasBox":
      table_env_zone_val = gold_prod_tb_ChristmasBox
  if ambiente == "prod" and zona == "gold" and table == "AnnualBonus":
      table_env_zone_val = gold_prod_tb_AnnualBonus  
  if ambiente == "prod" and zona == "gold" and table == "Training":
      table_env_zone_val = gold_prod_tb_Training
  if ambiente == "prod" and zona == "gold" and table == "MipHistorico":
      table_env_zone_val = gold_prod_tb_MipHistorico 

  return str(table_env_zone_val)
