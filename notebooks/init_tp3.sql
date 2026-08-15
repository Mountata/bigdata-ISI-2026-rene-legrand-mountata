-- ============================================================================
-- TP3 - Big Data Engineering - Master 1 - DMI/FST/UCAD - Prof. S. Ndiaye
-- Initialisation des bases MySQL du TP3
--
-- A executer UNE FOIS avant le TP, en tant que root :
--   - MySQL Workbench : File > Open SQL Script > init_tp3.sql, puis l'eclair
--   - ou en ligne de commande : mysql -u root -p < init_tp3.sql
--
-- Ce script cree :
--   1. ecommerce_crm       : la base du service client (table clients)
--   2. ecommerce_ventes    : la base de la plateforme de vente (table commandes)
--   3. ecommerce_analytics : une base vide ou Spark ecrira ses resultats
--   4. spark_user          : l'utilisateur MySQL que PySpark utilisera
--
-- Contexte (fil rouge du cours) : notre plateforme e-commerce senegalaise a
-- grandi. Le service client gere SA base (CRM) ; l'equipe de la plateforme
-- gere LA SIENNE (ventes). Deux bases separees, deux equipes -- et aucune
-- cle etrangere possible entre les deux. C'est PySpark qui fera le pont.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 0. Repartir de zero (le script est re-executable a volonte)
-- ---------------------------------------------------------------------------
DROP DATABASE IF EXISTS ecommerce_crm;
DROP DATABASE IF EXISTS ecommerce_ventes;
DROP DATABASE IF EXISTS ecommerce_analytics;

-- ---------------------------------------------------------------------------
-- 1. Base n. 1 : ecommerce_crm -- le referentiel clients du service client
-- ---------------------------------------------------------------------------
CREATE DATABASE ecommerce_crm
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE ecommerce_crm;

CREATE TABLE clients (
  client_id        VARCHAR(4)   NOT NULL,
  nom              VARCHAR(50)  NOT NULL,
  ville            VARCHAR(30)  NOT NULL,
  telephone        VARCHAR(20)  NOT NULL,
  email            VARCHAR(60)  NULL,          -- certains clients n'ont pas d'email
  date_inscription DATE         NOT NULL,
  PRIMARY KEY (client_id)
) ENGINE = InnoDB;

INSERT INTO clients (client_id, nom, ville, telephone, email, date_inscription) VALUES
  ('C001', 'Awa Diop',       'Dakar',       '77 123 45 67', 'awa.diop@gmail.com',        '2024-09-12'),
  ('C002', 'Moussa Ndiaye',  'Thies',       '78 234 56 78', 'moussa.ndiaye@yahoo.fr',    '2024-10-03'),
  ('C003', 'Fatou Sall',     'Dakar',       '76 345 67 89', 'fatou.sall@gmail.com',      '2024-11-21'),
  ('C004', 'Cheikh Ba',      'Dakar',       '70 456 78 90', NULL,                        '2025-01-15'),
  ('C005', 'Aissatou Diallo','Saint-Louis', '75 567 89 01', 'aissatou.diallo@hotmail.com','2025-02-08'),
  ('C006', 'Ibrahima Fall',  'Kaolack',     '77 678 90 12', 'ibrahima.fall@gmail.com',   '2025-03-19'),
  ('C007', 'Ousmane Sarr',   'Ziguinchor',  '78 789 01 23', 'ousmane.sarr@gmail.com',    '2025-04-27'),
  ('C008', 'Khady Gueye',    'Dakar',       '76 890 12 34', 'khady.gueye@yahoo.fr',      '2025-05-30'),
  ('C009', 'Mamadou Sy',     'Touba',       '70 901 23 45', NULL,                        '2025-06-14'),
  ('C010', 'Adama Kane',     'Rufisque',    '77 012 34 56', 'adama.kane@gmail.com',      '2025-07-22'),
  ('C011', 'Bineta Mbaye',   'Mbour',       '75 123 45 67', 'bineta.mbaye@gmail.com',    '2025-08-09'),
  ('C012', 'Serigne Diouf',  'Louga',       '78 210 43 65', 'serigne.diouf@hotmail.com', '2025-09-05');

-- ---------------------------------------------------------------------------
-- 2. Base n. 2 : ecommerce_ventes -- les commandes de la plateforme
--    NB : pas de cle etrangere vers clients -- la table est dans une AUTRE
--    base, geree par une AUTRE equipe. L'integrite se verifiera... avec Spark.
-- ---------------------------------------------------------------------------
CREATE DATABASE ecommerce_ventes
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE ecommerce_ventes;

CREATE TABLE commandes (
  commande_id    VARCHAR(5)  NOT NULL,
  client_id      VARCHAR(4)  NOT NULL,   -- reference LOGIQUE vers ecommerce_crm.clients
  produit_id     VARCHAR(4)  NOT NULL,   -- reference LOGIQUE vers le catalogue (CSV du TP)
  montant_fcfa   INT         NOT NULL,
  moyen_paiement VARCHAR(20) NOT NULL,
  statut         VARCHAR(15) NOT NULL,
  date_commande  DATE        NOT NULL,
  PRIMARY KEY (commande_id)
) ENGINE = InnoDB;

INSERT INTO commandes (commande_id, client_id, produit_id, montant_fcfa, moyen_paiement, statut, date_commande) VALUES
  ('CMD01', 'C001', 'P001', 145000, 'Orange Money', 'livree',   '2025-10-02'),
  ('CMD02', 'C003', 'P002',  25000, 'Wave',         'livree',   '2025-10-05'),
  ('CMD03', 'C002', 'P010',   8500, 'especes',      'livree',   '2025-10-07'),
  ('CMD04', 'C001', 'P003',  32000, 'Orange Money', 'livree',   '2025-10-12'),
  ('CMD05', 'C008', 'P005',  57500, 'carte',        'livree',   '2025-10-15'),
  ('CMD06', 'C005', 'P007',  12000, 'Wave',         'en_cours', '2025-10-18'),
  ('CMD07', 'C004', 'P008',  74500, 'Orange Money', 'livree',   '2025-10-21'),
  ('CMD08', 'C010', 'P009',  15500, 'Wave',         'livree',   '2025-10-24'),
  ('CMD09', 'C006', 'P006',  39000, 'especes',      'annulee',  '2025-10-27'),
  ('CMD10', 'C003', 'P004',  28500, 'Orange Money', 'livree',   '2025-11-01'),
  ('CMD11', 'C009', 'P010',   8500, 'especes',      'livree',   '2025-11-04'),
  ('CMD12', 'C012', 'P008',  74500, 'carte',        'livree',   '2025-11-08'),
  ('CMD13', 'C001', 'P002',  25000, 'Wave',         'livree',   '2025-11-11'),
  ('CMD14', 'C008', 'P006',  39000, 'Orange Money', 'en_cours', '2025-11-14'),
  ('CMD15', 'C999', 'P004',  28500, 'Wave',         'livree',   '2025-11-17'),
  ('CMD16', 'C002', 'P001', 145000, 'Orange Money', 'livree',   '2025-11-20'),
  ('CMD17', 'C005', 'P007',  12000, 'especes',      'annulee',  '2025-11-23'),
  ('CMD18', 'C004', 'P003',  32000, 'Wave',         'livree',   '2025-11-26'),
  ('CMD19', 'C010', 'P001', 145000, 'carte',        'livree',   '2025-11-29'),
  ('CMD20', 'C006', 'P010',   8500, 'Orange Money', 'livree',   '2025-12-02'),
  ('CMD21', 'C003', 'P002',  25000, 'Wave',         'en_cours', '2025-12-05'),
  ('CMD22', 'C009', 'P005',  57500, 'Orange Money', 'livree',   '2025-12-08'),
  ('CMD23', 'C012', 'P007',  12000, 'especes',      'livree',   '2025-12-11'),
  ('CMD24', 'C001', 'P008',  74500, 'carte',        'livree',   '2025-12-14'),
  ('CMD25', 'C008', 'P009',  15500, 'Wave',         'annulee',  '2025-12-17');

-- ---------------------------------------------------------------------------
-- 3. Base n. 3 : ecommerce_analytics -- vide : Spark y ecrira ses resultats
-- ---------------------------------------------------------------------------
CREATE DATABASE ecommerce_analytics
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- 4. L'utilisateur MySQL de PySpark
--    - lecture seule sur les bases metier (bonne pratique : moindre privilege)
--    - tous droits sur la base analytics (Spark doit pouvoir y creer des tables)
--    - le notebook n'aura ainsi JAMAIS besoin du mot de passe root
-- ---------------------------------------------------------------------------
DROP USER IF EXISTS 'spark_user'@'%';
CREATE USER 'spark_user'@'%' IDENTIFIED BY 'Ucad2026!';

GRANT SELECT ON ecommerce_crm.*       TO 'spark_user'@'%';
GRANT SELECT ON ecommerce_ventes.*    TO 'spark_user'@'%';
GRANT ALL    ON ecommerce_analytics.* TO 'spark_user'@'%';
FLUSH PRIVILEGES;

-- ---------------------------------------------------------------------------
-- 5. Verifications (les 4 requetes doivent afficher : 12, 25, spark_user, 3)
-- ---------------------------------------------------------------------------
SELECT COUNT(*) AS nb_clients   FROM ecommerce_crm.clients;
SELECT COUNT(*) AS nb_commandes FROM ecommerce_ventes.commandes;
SELECT user     AS utilisateur  FROM mysql.user WHERE user = 'spark_user';
SELECT COUNT(*) AS nb_bases     FROM information_schema.schemata
 WHERE schema_name LIKE 'ecommerce%';
