import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, count, expr, sum, avg

# facilite la documentation et le travail d'equipe
def analyser_et_sauvegarder_donnees(chemin_client, chemin_produit, chemin_vente, chemin_ecriture):
    """
    Analyse les données de vente, client et produit, effectue des jointures, des calculs et sauvegarde les résultats.

    Args:
        chemin_client (str): Chemin vers le fichier CSV des clients.
        chemin_produit (str): Chemin vers le fichier CSV des produits.
        chemin_vente (str): Chemin vers le fichier CSV des ventes.
        chemin_ecriture (str): Chemin vers le répertoire où les résultats seront sauvegardés.
    """
    # Créer une session Spark
    # Utiliser .master("local[*]") pour exécuter Spark en mode local avec tous les cœurs disponibles
    spark = SparkSession.builder.appName("GestionDonneesAvancee") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .master("local[*]") \
        .getOrCreate()

    # --- Schémas ---
    # S'assurer que les noms des colonnes dans le schéma correspondent exactement
    # aux en-têtes des fichiers CSV après le renommage implicite par `header=True`.
    schema_client = StructType([
        StructField("N_Client", IntegerType(), True),
        StructField("Nom_Client", StringType(), True),
        StructField("Adresse_Client", StringType(), True),
        StructField("Ville_Client", StringType(), True)
    ])

    schema_produit = StructType([
        StructField("Code_produit", IntegerType(), True),
        StructField("nom", StringType(), True),
        StructField("desc_produit", StringType(), True),
        StructField("categorie", StringType(), True),
        StructField("desc_cate", StringType(), True),
        StructField("prix", FloatType(), True)
    ])

    schema_vente = StructType([
        StructField("Code_Produit", IntegerType(), True),
        StructField("N_Client", IntegerType(), True),
        StructField("Quantite", IntegerType(), True),
        StructField("Prix", FloatType(), True)
    ])

    # --- Vérification de l'existence des fichiers et création du répertoire de sortie ---
    # Vérification des chemins d'entrée
    if not os.path.exists(chemin_client):
        print(f"Erreur: Le fichier client n'existe pas ou le chemin est incorrect : {chemin_client}")
        spark.stop()
        return
    if not os.path.exists(chemin_produit):
        print(f"Erreur: Le fichier produit n'existe pas ou le chemin est incorrect : {chemin_produit}")
        spark.stop()
        return
    if not os.path.exists(chemin_vente):
        print(f"Erreur: Le fichier vente n'existe pas ou le chemin est incorrect : {chemin_vente}")
        spark.stop()
        return

    # Création du répertoire de sortie s'il n'existe pas
    if not os.path.exists(chemin_ecriture):
        try:
            os.makedirs(chemin_ecriture)
            print(f"Répertoire de sortie créé : {chemin_ecriture}")
        except OSError as e:
            print(f"Erreur lors de la création du répertoire de sortie {chemin_ecriture}: {e}")
            spark.stop()
            return
    else:
        print(f"Le répertoire de sortie existe déjà : {chemin_ecriture}")

    # --- Lecture des fichiers CSV avec gestion d'exceptions ---
    try:
        df_client = spark.read.csv(chemin_client, header=True, schema=schema_client, sep=';')
        df_produit = spark.read.csv(chemin_produit, header=True, schema=schema_produit, sep=';')
        df_vente = spark.read.csv(chemin_vente, header=True, schema=schema_vente, sep=';')

        print("Fichiers CSV lus avec succès.")

    except Exception as e:
        print(f"Erreur lors de la lecture des fichiers CSV : {e}")
        spark.stop()
        return

    # --- Renommage des colonnes ---
    df_client = df_client.withColumnRenamed("N_Client", "ID_Client") \
                             .withColumnRenamed("Nom_Client", "Nom") \
                             .withColumnRenamed("Adresse_Client", "Adresse") \
                             .withColumnRenamed("Ville_Client", "Ville")

    df_produit = df_produit.withColumnRenamed("Code_produit", "ID_Produit") \
                               .withColumnRenamed("nom", "Nom_Produit") \
                               .withColumnRenamed("desc_produit", "Description_Produit")\
                               .withColumnRenamed("categorie", "Categorie_Produit")\
                               .withColumnRenamed("desc_cate", "Description_Categorie_Produit")\
                               .withColumnRenamed("prix", "Prix_Unitaire")

    df_vente = df_vente.withColumnRenamed("Code_Produit", "ID_Produit") \
                            .withColumnRenamed("N_Client", "ID_Client") \
                            .withColumnRenamed("Quantite", "Quantite_Vendue") \
                            .withColumnRenamed("Prix", "Prix_Vente_Unitaire")

    # Afficher les schémas après renommage pour vérifier
    print("\nSchéma de df_client après renommage:")
    df_client.printSchema()
    print("\nSchéma de df_produit après renommage:")
    df_produit.printSchema()
    print("\nSchéma de df_vente après renommage:")
    df_vente.printSchema()

    # --- Jointures ---
    df_ventes_completes = df_vente.join(df_client, on="ID_Client", how="inner") \
                                  .join(df_produit, on="ID_Produit", how="inner")

    # Calcul du chiffre d'affaires pour chaque vente
    df_ventes_completes = df_ventes_completes.withColumn(
        "Chiffre_Affaires_Ligne", expr("Quantite_Vendue * Prix_Vente_Unitaire")
    )

    print("\n--- Aperçu du DataFrame des ventes complètes avec Chiffre d'Affaires par ligne ---")
    df_ventes_completes.show(5, truncate=False)

    # --- Analyses et Calculs ---

    # Regrouper par produit pour obtenir les statistiques de vente
    df_stats_produit = df_ventes_completes.groupBy("ID_Produit", "Nom_Produit").agg(
        count("*").alias("Nombre_Ventes"),
        sum("Quantite_Vendue").alias("Total_Quantite_Vendue"),
        sum("Chiffre_Affaires_Ligne").alias("Chiffre_Affaires_Total_Produit")
    )

    # --- Identifier les trois produits les plus vendus (par nombre de ventes) ---
    df_top_3_produits_ventes = df_stats_produit.orderBy(col("Nombre_Ventes").desc()).limit(3)
    top_3_noms_produits_ventes = [row.Nom_Produit for row in df_top_3_produits_ventes.collect()]

    print("\n--- Les trois produits les plus vendus (par nombre de ventes) ---")
    df_top_3_produits_ventes.show(truncate=False)

    # --- Filtrer la table des ventes pour afficher uniquement ces trois produits ---
    df_ventes_top_3_produits = df_ventes_completes.filter(col("Nom_Produit").isin(top_3_noms_produits_ventes))

    print("\n--- Détails des ventes pour les trois produits les plus vendus ---")
    df_ventes_top_3_produits.show(truncate=False)

    # --- Identifier les trois produits avec le chiffre d'affaires le plus élevé ---
    df_top_3_produits_ca = df_stats_produit.orderBy(col("Chiffre_Affaires_Total_Produit").desc()).limit(3)
    top_3_noms_produits_ca = [row.Nom_Produit for row in df_top_3_produits_ca.collect()]
    df_ventes_top_3_ca_produits = df_ventes_completes.filter(col("Nom_Produit").isin(top_3_noms_produits_ca))

    print("\n--- Les trois produits avec le plus grand chiffre d'affaires ---")
    df_top_3_produits_ca.show(truncate=False)
    print("\n--- Détails des ventes pour les trois produits avec le plus grand chiffre d'affaires ---")
    df_ventes_top_3_ca_produits.show(truncate=False)

    # --- Calcul du nombre total de ventes par ville ---
    df_ventes_par_ville = df_ventes_completes.groupBy("Ville").agg(
        count("*").alias("Nombre_Total_Ventes"),
        sum("Chiffre_Affaires_Ligne").alias("Chiffre_Affaires_Total_Ville")
    )
    print("\n--- Nombre total de ventes et Chiffre d'Affaires par ville ---")
    df_ventes_par_ville.show(truncate=False)

    # --- Sauvegarde des résultats ---
    try:
        df_ventes_top_3_produits.write.csv(path=os.path.join(chemin_ecriture, "ventes_top_3_produits"), header=True, mode="overwrite")
        print(f"\nTable des ventes pour les trois produits les plus vendus écrite avec succès dans : {os.path.join(chemin_ecriture, 'ventes_top_3_produits')}")

        df_top_3_produits_ca.write.csv(path=os.path.join(chemin_ecriture, "top_3_chiffre_affaires"), header=True, mode="overwrite")
        print(f"Table des trois produits avec le plus grand chiffre d'affaires écrite avec succès dans : {os.path.join(chemin_ecriture, 'top_3_chiffre_affaires')}")

        df_ventes_par_ville.write.csv(path=os.path.join(chemin_ecriture, "ventes_par_ville"), header=True, mode="overwrite")
        print(f"Table du nombre total de ventes par ville écrite avec succès dans : {os.path.join(chemin_ecriture, 'ventes_par_ville')}")

    except Exception as e:
        print(f"Erreur lors de l'écriture des tables : {e}")
    finally:
        spark.stop()
        print("Session Spark arrêtée.")

if __name__ == "__main__":
    # --- Configuration des chemins de fichiers ---
    chemin_client = "C:/data/Client.csv"
    chemin_produit = "C:/data/Produit.csv"
    chemin_vente = "C:/data/Vente.csv"
    chemin_ecriture = "C:/data/output_data/"  # Répertoire pour les fichiers de sortie

    # Appeler la fonction principale
    analyser_et_sauvegarder_donnees(chemin_client, chemin_produit, chemin_vente, chemin_ecriture)
