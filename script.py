import pyspark.sql.functions as f
from pyspark.sql.types import *
from unidecode import unidecode

#https://dadosabertos.tse.jus.br/dataset/candidatos-2024 

df_candidatos = spark.read.option("header", "true").option("sep", ";").option("encoding", "cp1252").csv("/FileStore/tables/consulta_cand_2024_SP.csv")

df_candidatos = df_candidatos.withColumnRenamed("SQ_CANDIDATO", "id_candidato")
df_candidatos = df_candidatos.withColumnRenamed("NM_CANDIDATO", "nome")
df_candidatos = df_candidatos.withColumnRenamed("NM_URNA_CANDIDATO", "nome_urna")
df_candidatos = df_candidatos.withColumnRenamed("NM_SOCIAL_CANDIDATO", "nome_social")
df_candidatos = df_candidatos.withColumnRenamed("SG_PARTIDO", "partido")
df_candidatos = df_candidatos.withColumnRenamed("NM_UE", "cidade")
df_candidatos = df_candidatos.withColumnRenamed("SG_UF", "estado")
df_candidatos = df_candidatos.withColumnRenamed("DS_CARGO", "cargo")

#pip install unidecode



def remove_acentos(text):
    return unidecode(text) if text is not None else None

remove_acentos_udf_candidatos = udf(remove_acentos, StringType())

df_candidatos = df_candidatos.withColumn("nome", remove_acentos_udf_candidatos(f.col("nome")))
df_candidatos = df_candidatos.withColumn("nome_urna", remove_acentos_udf_candidatos(f.col("nome_urna")))
df_candidatos = df_candidatos.withColumn("nome_social", remove_acentos_udf_candidatos(f.col("nome_social")))
df_candidatos = df_candidatos.withColumn("cargo", remove_acentos_udf_candidatos(f.col("cargo")))
df_candidatos = df_candidatos.withColumn("partido", remove_acentos_udf_candidatos(f.col("partido")))
df_candidatos = df_candidatos.withColumn("cidade", remove_acentos_udf_candidatos(f.col("cidade")))
df_candidatos = df_candidatos.withColumn("estado", remove_acentos_udf_candidatos(f.col("estado")))

df_candidatos = df_candidatos.select(f.col("id_candidato"),f.col("nome"), f.col("nome_urna"), f.col("nome_social"), f.col("cargo"), f.col("partido"),f.col("cidade"),f.col("estado"))

df_candidatos_bens = spark.read.option("header", "true").option("sep", ";").option("encoding", "cp1252").csv("/FileStore/tables/bem_candidato_2024_SP.csv")

df_candidatos_bens = df_candidatos_bens.withColumnRenamed("SQ_CANDIDATO", "num_candidato")
df_candidatos_bens = df_candidatos_bens.withColumnRenamed("DS_TIPO_BEM_CANDIDATO", "bem_candidato")
df_candidatos_bens = df_candidatos_bens.withColumnRenamed("VR_BEM_CANDIDATO", "valor_bem_candidato")
df_candidatos_bens = df_candidatos_bens.select(df_candidatos_bens.num_candidato, df_candidatos_bens.bem_candidato, df_candidatos_bens.valor_bem_candidato )

df_candidatos_bens_candidatos = df_candidatos.join(df_candidatos_bens, df_candidatos.id_candidato==df_candidatos_bens.num_candidato, "inner")

df_candidatos_bens_candidatos = df_candidatos_bens_candidatos.withColumn("valor_bem_candidato", f.regexp_replace(f.col("valor_bem_candidato"), ",", ".").cast(DecimalType(10,2)))

df_candidatos_bens_candidatos.createOrReplaceTempView("joined_table")

spark.sql("""
          SELECT num_candidato, nome, cargo,partido, SUM(valor_bem_candidato) AS patrimonio_total 
          FROM joined_table 
          WHERE cidade=="CIDADE"
          AND cargo =="PREFEITO"
          GROUP BY num_candidato, nome , cargo, partido
          ORDER BY patrimonio_total DESC;
          """).show(truncate=False)

