"""
Module de transformations pour le nettoyage des données clients.

Ce module contient les fonctions pures de transformation appliquées
sur les DataFrames PySpark pour nettoyer, normaliser et dédupliquer
les données clients.
"""

from pyspark.sql import DataFrame, functions as F
import unicodedata

# Table de correspondance caractère accentué -> caractère de base, utilisée par
# F.translate (fonction Spark NATIVE, pas une UDF Python). On évite ainsi tout
# aller-retour vers un worker Python pour cette transformation très fréquente :
# plus rapide, optimisable par Catalyst, et surtout robuste (une UDF Python peut
# planter selon la version de PySpark/Python/OS - "Python worker exited
# unexpectedly" est une panne connue avec certaines combinaisons sur Windows).
_ACCENTS_SRC = ("ÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïñòóôõöùúûüýÿĀāĂăĄą"
                "ĆćĈĉĊċČčĎďĒēĔĕĖėĘęĚěĜĝĞğĠġĢģĤĥĨĩĪīĬĭĮįİĴĵĶķĹĺĻļĽľŃńŅņŇň"
                "ŌōŎŏŐőŔŕŖŗŘřŚśŜŝŞşŠšŢţŤťŨũŪūŬŭŮůŰűŲųŴŵŶŷŸŹźŻżŽžſ")
_ACCENTS_DST = ("AAAAAACEEEEIIIINOOOOOUUUUYaaaaaaceeeeiiiinooooouuuuyyAaAaAaCc"
                "CcCcCcDdEeEeEeEeEeGgGgGgGgHhIiIiIiIiIJjKkLlLlLlNnNnNnOoOoOo"
                "RrRrRrSsSsSsSsTtTtUuUuUuUuUuUuWwYyYZzZzZzs")


def sans_accent(s):
    """
    Supprime les accents d'une chaîne de caractères.

    Conservée pour usage ponctuel côté Python pur (ex. dans un test ou un
    script hors Spark) ; le pipeline lui-même utilise F.translate (voir
    normaliser_ville), pas cette fonction, pour éviter toute UDF.

    Args:
        s (str): Chaîne à normaliser

    Returns:
        str: Chaîne sans accents
    """
    if s is None:
        return None
    nfkd = unicodedata.normalize('NFKD', str(s))
    return ''.join([c for c in nfkd if not unicodedata.combining(c)])


def unifier_manquants(df: DataFrame) -> DataFrame:
    """
    Transforme les emails vides ou "N/A" en null.

    Args:
        df (DataFrame): DataFrame d'entrée

    Returns:
        DataFrame: DataFrame avec emails normalisés
    """
    return df.withColumn(
        "email",
        F.when(
            (F.trim(F.col("email")) == "") |
            (F.upper(F.trim(F.col("email"))) == "N/A"),
            F.lit(None)
        ).otherwise(F.trim(F.col("email")))
    )


def normaliser_email(df: DataFrame) -> DataFrame:
    """
    Normalise l'email (minuscule, trim) et ajoute un drapeau de validité.

    Args:
        df (DataFrame): DataFrame d'entrée

    Returns:
        DataFrame: DataFrame avec email normalisé et validé
    """
    df = df.withColumn(
        "email",
        F.lower(F.trim(F.col("email")))
    )
    return df.withColumn(
        "email_valide",
        F.when(F.col("email").isNull(), F.lit(None))
         .when(
             F.col("email").rlike(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"),
             F.lit(True)
         )
         .otherwise(F.lit(False))
    )


def normaliser_ville(df: DataFrame) -> DataFrame:
    """
    Normalise le nom de la ville (trim + initcap) et crée une clé sans accent.

    Args:
        df (DataFrame): DataFrame d'entrée

    Returns:
        DataFrame: DataFrame avec ville normalisée et clé sans accent
    """
    return df.withColumn(
        "ville",
        F.initcap(F.trim(F.col("ville")))
    ).withColumn(
        "ville_norm",
        F.translate(F.lower(F.trim(F.col("ville"))), _ACCENTS_SRC, _ACCENTS_DST)
    )


def normaliser_telephone(df: DataFrame) -> DataFrame:
    """
    Normalise les numéros de téléphone sénégalais.

    On retire d'abord tout caractère non numérique (espaces, tirets,
    "+"), puis un éventuel préfixe pays "221" en tête, avant de
    valider le format attendu (9 chiffres, préfixe 70/75/76/77/78).

    Args:
        df (DataFrame): DataFrame d'entrée

    Returns:
        DataFrame: DataFrame avec téléphone normalisé et validé
    """
    tel = F.regexp_replace(F.col("telephone"), r"[^0-9]", "")
    tel = F.regexp_replace(tel, r"^221", "")
    return (
        df.withColumn("telephone", tel)
          .withColumn(
              "telephone_valide",
              F.col("telephone").rlike(r"^(70|75|76|77|78)\d{7}$")
          )
    )


def valider_naissance(df: DataFrame) -> DataFrame:
    """
    Valide la date de naissance (entre le 1920-01-01 et aujourd'hui).

    La comparaison se fait sur la date complète (pas seulement
    l'année), pour rejeter correctement une date future même si
    elle tombe dans l'année en cours.

    Args:
        df (DataFrame): DataFrame d'entrée

    Returns:
        DataFrame: DataFrame avec date de naissance validée
    """
    d = F.to_date(F.col("date_naissance"), "yyyy-MM-dd")
    return df.withColumn(
        "date_naissance",
        F.when(
            d.isNotNull() &
            (d >= F.lit("1920-01-01")) &
            (d <= F.current_date()),
            d
        ).otherwise(F.lit(None))
    )


def dedupliquer_clients(df: DataFrame) -> DataFrame:
    """
    Déduplique les clients après normalisation.

    Args:
        df (DataFrame): DataFrame d'entrée

    Returns:
        DataFrame: DataFrame dédupliqué
    """
    df_sans_doublons = df.dropDuplicates()
    return df_sans_doublons.dropDuplicates(["customer_id"])


def nettoyer_clients(df: DataFrame) -> DataFrame:
    """
    Pipeline complet de nettoyage des clients.

    Args:
        df (DataFrame): DataFrame brut

    Returns:
        DataFrame: DataFrame nettoyé
    """
    return (df
        .transform(unifier_manquants)
        .transform(normaliser_email)
        .transform(normaliser_ville)
        .transform(normaliser_telephone)
        .transform(valider_naissance)
        .transform(dedupliquer_clients))