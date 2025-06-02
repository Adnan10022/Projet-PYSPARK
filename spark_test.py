from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, sum, count, expr

def analyser_et_sauvegarder_donnees(chemin_client, chemin_produit, chemin_vente, chemin_ecriture):
    spark = SparkSession.builder.appName("AnalyseVentes").master("local[*]").getOrCreate()

    # Schémas simplifiés
    schema_client = StructType([
        StructField("N_Client", IntegerType(), True),
        StructField("Ville_Client", StringType(), True)
    ])
    schema_produit = StructType([
        StructField("Code_produit", IntegerType(), True),
        StructField("nom", StringType(), True)
    ])
    schema_vente = StructType([
        StructField("Code_Produit", IntegerType(), True),
        StructField("N_Client", IntegerType(), True),
        StructField("Quantite", IntegerType(), True),
        StructField("Prix", FloatType(), True)
    ])

    # Lecture des fichiers CSV
    df_client = spark.read.csv(chemin_client, header=True, schema=schema_client, sep=';')
    df_produit = spark.read.csv(chemin_produit, header=True, schema=schema_produit, sep=';')
    df_vente = spark.read.csv(chemin_vente, header=True, schema=schema_vente, sep=';')

    # Renommer 
    df_client = df_client.withColumnRenamed("N_Client", "ID_Client").withColumnRenamed("Ville_Client", "Ville")
    df_produit = df_produit.withColumnRenamed("Code_produit", "ID_Produit").withColumnRenamed("nom", "Nom_Produit")
    df_vente = df_vente.withColumnRenamed("Code_Produit", "ID_Produit") \
                       .withColumnRenamed("N_Client", "ID_Client") \
                       .withColumnRenamed("Quantite", "Quantite_Vendue") \
                       .withColumnRenamed("Prix", "Prix_Unitaire")

    # Jointure
    df_ventes = df_vente.join(df_client, "ID_Client").join(df_produit, "ID_Produit")

    # Calcul du chiffre d'affaires
    df_ventes = df_ventes.withColumn("CA", expr("Quantite_Vendue * Prix_Unitaire"))

    # Top 3 produits par nombre de ventes
    top_3_ventes = df_ventes.groupBy("Nom_Produit").agg(count("*").alias("Nb_ventes")) \
                            .orderBy(col("Nb_ventes").desc()).limit(3)

    print(" Top 3 produits par nombre de ventes :")
    top_3_ventes.show(truncate=False)

    # Top 3 produits par chiffre d'affaires
    top_3_ca = df_ventes.groupBy("Nom_Produit").agg(sum("CA").alias("Chiffre_Affaires")) \
                        .orderBy(col("Chiffre_Affaires").desc()).limit(3)

    print("Top 3 produits par chiffre d'affaires :")
    top_3_ca.show(truncate=False)

    # Ventes par ville
    ventes_ville = df_ventes.groupBy("Ville").agg(sum("CA").alias("Chiffre_Affaires"))

    print(" Ventes par ville :")
    ventes_ville.show(truncate=False)

    # Sauvegarde
    top_3_ventes.write.csv(chemin_ecriture + "/top_3_ventes", header=True, mode="overwrite")
    top_3_ca.write.csv(chemin_ecriture + "/top_3_ca", header=True, mode="overwrite")
    ventes_ville.write.csv(chemin_ecriture + "/ventes_par_ville", header=True, mode="overwrite")

    spark.stop()

# Exemple d’appel
if __name__ == "__main__":
    analyser_et_sauvegarder_donnees(
        "C:/data/Client.csv",
        "C:/data/Produit.csv",
        "C:/data/Vente.csv",
        "C:/data/output_data"
    )
