"""
Tests unitaires pour le module de transformations.
Utilise pytest + chispa pour la comparaison des DataFrames.
"""

import pytest
from chispa import assert_df_equality
from src.transformations import (
    unifier_manquants,
    normaliser_email,
    normaliser_ville,
    normaliser_telephone,
    valider_naissance,
    dedupliquer_clients,
    nettoyer_clients
)


def test_unifier_manquants(spark):
    """Test de la fonction unifier_manquants."""
    entree = spark.createDataFrame([
        ("1", "client@mail.com"),
        ("2", ""),
        ("3", "N/A"),
        ("4", "  "),
    ], ["customer_id", "email"])

    resultat = unifier_manquants(entree)

    attendu = spark.createDataFrame([
        ("1", "client@mail.com"),
        ("2", None),
        ("3", None),
        ("4", None),
    ], ["customer_id", "email"])

    assert_df_equality(resultat, attendu)


def test_normaliser_email(spark):
    """Test de la fonction normaliser_email."""
    entree = spark.createDataFrame([
        ("1", "  Client@Mail.com  "),
        ("2", "client@mail"),
        ("3", "client@mail.com"),
    ], ["customer_id", "email"])

    resultat = normaliser_email(entree)

    resultat_collect = resultat.select("email", "email_valide").collect()

    assert resultat_collect[0]["email"] == "client@mail.com"
    assert resultat_collect[0]["email_valide"] == True

    assert resultat_collect[1]["email"] == "client@mail"
    assert resultat_collect[1]["email_valide"] == False

    assert resultat_collect[2]["email"] == "client@mail.com"
    assert resultat_collect[2]["email_valide"] == True


def test_normaliser_email_null(spark):
    """Un email null doit rester null, avec un drapeau de validité null (pas False)."""
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("email", StringType(), True),
    ])
    entree = spark.createDataFrame([("1", None)], schema=schema)

    resultat = normaliser_email(entree)
    row = resultat.select("email", "email_valide").collect()[0]

    assert row["email"] is None
    assert row["email_valide"] is None


def test_normaliser_ville(spark):
    """Test de la fonction normaliser_ville."""
    entree = spark.createDataFrame([
        ("1", " DAKAR "),
        ("2", "  thiès  "),
        ("3", "THIES"),
        ("4", "Saint-Louis"),
    ], ["customer_id", "ville"])

    resultat = normaliser_ville(entree)

    resultat_collect = resultat.select("ville", "ville_norm").collect()

    assert resultat_collect[0]["ville"] == "Dakar"
    assert resultat_collect[0]["ville_norm"] == "dakar"

    assert resultat_collect[1]["ville"] == "Thiès"
    assert resultat_collect[1]["ville_norm"] == "thies"

    assert resultat_collect[2]["ville"] == "Thies"
    assert resultat_collect[2]["ville_norm"] == "thies"


def test_normaliser_telephone(spark):
    """Test de la fonction normaliser_telephone.

    Couvre volontairement des formats bruts variés : chiffres collés,
    préfixe international "+221 ", séparateurs par tirets, un numéro
    trop court, et un préfixe opérateur invalide.
    """
    entree = spark.createDataFrame([
        ("1", "771234567"),
        ("2", "+221 77 123 45 67"),
        ("3", "78-123-45-67"),
        ("4", "70123456"),   # 8 chiffres = invalide
        ("5", "691234567"),  # mauvais préfixe
    ], ["customer_id", "telephone"])

    resultat = normaliser_telephone(entree)

    resultat_collect = resultat.select("telephone", "telephone_valide").collect()

    assert resultat_collect[0]["telephone"] == "771234567"
    assert resultat_collect[0]["telephone_valide"] == True

    assert resultat_collect[1]["telephone"] == "771234567"
    assert resultat_collect[1]["telephone_valide"] == True

    assert resultat_collect[2]["telephone"] == "781234567"
    assert resultat_collect[2]["telephone_valide"] == True

    assert resultat_collect[3]["telephone"] == "70123456"  # 8 chiffres
    assert resultat_collect[3]["telephone_valide"] == False

    assert resultat_collect[4]["telephone"] == "691234567"
    assert resultat_collect[4]["telephone_valide"] == False


def test_valider_naissance(spark):
    """Test de la fonction valider_naissance.

    NB : on utilise une date très lointaine (2099) pour représenter
    "le futur", afin que le test reste valable quelle que soit la
    date d'exécution (contrairement à une date fixe comme 2026, qui
    finit par devenir une date passée).
    """
    entree = spark.createDataFrame([
        ("1", "1990-01-01"),  # Valide
        ("2", "1919-12-31"),  # Trop ancien (< 1920)
        ("3", "2099-01-01"),  # Dans le futur
        ("4", "15/01/1990"),  # Format invalide
        ("5", "2000-02-30"),  # Date invalide (30 février n'existe pas)
    ], ["customer_id", "date_naissance"])

    resultat = valider_naissance(entree)

    resultat_collect = resultat.select("date_naissance").collect()

    assert resultat_collect[0]["date_naissance"] is not None  # Valide
    assert resultat_collect[1]["date_naissance"] is None      # < 1920
    assert resultat_collect[2]["date_naissance"] is None      # > aujourd'hui
    assert resultat_collect[3]["date_naissance"] is None      # Format invalide
    assert resultat_collect[4]["date_naissance"] is None      # Date invalide


def test_dedupliquer_clients(spark):
    """Test de la fonction dedupliquer_clients."""
    entree = spark.createDataFrame([
        ("1", "client1@mail.com", "Dakar"),
        ("2", "client2@mail.com", "Thies"),
        ("1", "client1@mail.com", "Dakar"),   # Doublon exact
        ("3", "client1@MAIL.com", "dakar"),   # Client distinct (id différent)
    ], ["customer_id", "email", "ville"])

    resultat = dedupliquer_clients(entree)

    # On attend 3 lignes (customer_id 1, 2, 3)
    assert resultat.count() == 3


def test_nettoyer_clients_pipeline(spark):
    """Test du pipeline complet de nettoyage."""
    entree = spark.createDataFrame([
        ("1", "Jean", "Dupont", " JEAN@MAIL.COM ", "77-123-45-67", " dakar ", "DAKAR", "1990-01-01", "2023-01-01"),
        ("2", "Marie", "Diop", "N/A", "78 123 45 67", "  thiès  ", "THIES", "1985-06-15", "2023-01-01"),
        ("3", "Pierre", "Fall", "", "70 12 34 56", " SAINT-LOUIS ", "SAINT-LOUIS", "2010-03-10", "2023-01-01"),
        ("1", "Jean", "Dupont", "jean@mail.com", "771234567", "Dakar", "DAKAR", "1990-01-01", "2023-01-01"),
    ], ["customer_id", "prenom", "nom", "email", "telephone", "ville", "region", "date_naissance", "date_inscription"])

    resultat = nettoyer_clients(entree)

    # On attend 3 lignes (customer_id 1, 2, 3)
    assert resultat.count() == 3

    resultat_collect = resultat.select("email", "ville_norm").collect()
    emails = [r["email"] for r in resultat_collect]
    ville_norms = [r["ville_norm"] for r in resultat_collect]

    # Pas de valeurs vides ou "N/A"
    assert None in emails or "N/A" not in emails
    assert "dakar" in ville_norms
    assert "thies" in ville_norms
    assert "saint-louis" in ville_norms