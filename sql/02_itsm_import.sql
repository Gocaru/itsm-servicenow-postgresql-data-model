-- =============================================================
-- 02_itsm_import.sql
-- Projeto ITSM (PostgreSQL) — Importação e Carga (ETL)
-- Inclui: staging (stg_itsm), carga do CSV e povoamento 3NF
-- =============================================================

SET search_path = public;

-- =========================
-- 1) Staging
-- =========================
DROP TABLE IF EXISTS stg_itsm CASCADE;

CREATE TABLE stg_itsm (
  "CI_Name" text NOT NULL,
  "CI_Cat" text NULL,
  "CI_Subcat" text NULL,
  "WBS" text NOT NULL,
  "Incident_ID" text NOT NULL,
  "Status" text NOT NULL,
  "Impact" text NOT NULL,
  "Urgency" text NOT NULL,
  "Priority" text NULL,
  "number_cnt" text NOT NULL,
  "Category" text NOT NULL,
  "KB_number" text NOT NULL,
  "Alert_Status" text NOT NULL,
  "No_of_Reassignments" text NULL,
  "Open_Time" text NOT NULL,
  "Reopen_Time" text NULL,
  "Resolved_Time" text NULL,
  "Close_Time" text NOT NULL,
  "Handle_Time_hrs" text NULL,
  "Closure_Code" text NULL,
  "No_of_Related_Interactions" text NULL,
  "Related_Interaction" text NOT NULL,
  "No_of_Related_Incidents" text NULL,
  "No_of_Related_Changes" text NULL,
  "Related_Change" text NULL
);

-- =========================
-- 2) Importação do CSV
-- =========================
-- Opção A (psql): ajustar o caminho e usar \copy
-- \copy stg_itsm FROM 'C:/CAMINHO/ITSM_data.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
--
-- Opção B (SQL / servidor): requer caminho acessível ao servidor PostgreSQL e permissões
-- COPY stg_itsm FROM '/caminho/no/servidor/ITSM_data.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- =========================
-- 3) Povoamento das dimensões e tabelas 3NF
-- =========================
-- Pré-requisito: executar 01_itsm_structure.sql antes deste ficheiro.

-- 3.1) Garantir Unknown (robustez)
INSERT INTO ci_category_dim (ci_cat_name) VALUES ('Unknown')
ON CONFLICT (ci_cat_name) DO NOTHING;

INSERT INTO ci_subcategory_dim (ci_subcat_name) VALUES ('Unknown')
ON CONFLICT (ci_subcat_name) DO NOTHING;

INSERT INTO priority_dim (priority_code) VALUES ('Unknown')
ON CONFLICT (priority_code) DO NOTHING;

INSERT INTO closure_code_dim (closure_code_name) VALUES ('Unknown')
ON CONFLICT (closure_code_name) DO NOTHING;

-- 3.2) WBS
INSERT INTO wbs (wbs_code)
SELECT DISTINCT TRIM("WBS")
FROM stg_itsm
WHERE "WBS" IS NOT NULL AND TRIM("WBS") <> ''
ON CONFLICT (wbs_code) DO NOTHING;

-- 3.3) CI categories/subcategories (normalização forte)
INSERT INTO ci_category_dim (ci_cat_name)
SELECT DISTINCT COALESCE(NULLIF(TRIM("CI_Cat"), ''), 'Unknown')
FROM stg_itsm
ON CONFLICT (ci_cat_name) DO NOTHING;

INSERT INTO ci_subcategory_dim (ci_subcat_name)
SELECT DISTINCT COALESCE(NULLIF(TRIM("CI_Subcat"), ''), 'Unknown')
FROM stg_itsm
ON CONFLICT (ci_subcat_name) DO NOTHING;

-- 3.4) Configuration items (1 por CI_Name)
INSERT INTO configuration_item (ci_code, ci_cat_id, ci_subcat_id)
SELECT DISTINCT
  TRIM(s."CI_Name") AS ci_code,
  c.ci_cat_id,
  sc.ci_subcat_id
FROM stg_itsm s
JOIN ci_category_dim c
  ON c.ci_cat_name = COALESCE(NULLIF(TRIM(s."CI_Cat"), ''), 'Unknown')
JOIN ci_subcategory_dim sc
  ON sc.ci_subcat_name = COALESCE(NULLIF(TRIM(s."CI_Subcat"), ''), 'Unknown')
WHERE s."CI_Name" IS NOT NULL AND TRIM(s."CI_Name") <> ''
ON CONFLICT (ci_code) DO NOTHING;

-- 3.5) Dimensões ITSM
INSERT INTO impact_dim (impact_code)
SELECT DISTINCT TRIM("Impact")
FROM stg_itsm
WHERE "Impact" IS NOT NULL AND TRIM("Impact") <> ''
ON CONFLICT (impact_code) DO NOTHING;

INSERT INTO urgency_dim (urgency_code)
SELECT DISTINCT TRIM("Urgency")
FROM stg_itsm
WHERE "Urgency" IS NOT NULL AND TRIM("Urgency") <> ''
ON CONFLICT (urgency_code) DO NOTHING;

INSERT INTO incident_status_dim (status_name)
SELECT DISTINCT TRIM("Status")
FROM stg_itsm
WHERE "Status" IS NOT NULL AND TRIM("Status") <> ''
ON CONFLICT (status_name) DO NOTHING;

INSERT INTO incident_category_dim (category_name)
SELECT DISTINCT TRIM("Category")
FROM stg_itsm
WHERE "Category" IS NOT NULL AND TRIM("Category") <> ''
ON CONFLICT (category_name) DO NOTHING;

INSERT INTO closure_code_dim (closure_code_name)
SELECT DISTINCT COALESCE(NULLIF(TRIM("Closure_Code"), ''), 'Unknown')
FROM stg_itsm
ON CONFLICT (closure_code_name) DO NOTHING;

INSERT INTO kb_article (kb_number)
SELECT DISTINCT TRIM("KB_number")
FROM stg_itsm
WHERE "KB_number" IS NOT NULL AND TRIM("KB_number") <> ''
ON CONFLICT (kb_number) DO NOTHING;

INSERT INTO priority_dim (priority_code)
SELECT DISTINCT COALESCE(NULLIF(TRIM("Priority"), ''), 'Unknown')
FROM stg_itsm
ON CONFLICT (priority_code) DO NOTHING;

-- 3.6) Matriz (impact, urgency) -> priority
INSERT INTO priority_matrix (impact_id, urgency_id, priority_id)
SELECT DISTINCT
  i.impact_id,
  u.urgency_id,
  p.priority_id
FROM stg_itsm s
JOIN impact_dim i
  ON i.impact_code = TRIM(s."Impact")
JOIN urgency_dim u
  ON u.urgency_code = TRIM(s."Urgency")
JOIN priority_dim p
  ON p.priority_code = COALESCE(NULLIF(TRIM(s."Priority"), ''), 'Unknown')
ON CONFLICT (impact_id, urgency_id) DO NOTHING;

-- 3.7) Incidentes
TRUNCATE TABLE incident;

INSERT INTO incident (
  incident_id, wbs_id, ci_id,
  status_id, category_id,
  impact_id, urgency_id, priority_id,
  kb_id, closure_code_id,
  no_of_reassignments,
  no_of_related_interactions, related_interaction_raw,
  no_of_related_incidents,
  no_of_related_changes, related_change_raw,
  open_time, reopen_time, resolved_time, close_time,
  handle_time_hours,
  number_cnt,
  alert_status
)
SELECT
  TRIM(s."Incident_ID") AS incident_id,

  w.wbs_id,
  ci.ci_id,

  st.status_id,
  cat.category_id,

  imp.impact_id,
  urg.urgency_id,
  pri.priority_id,

  kb.kb_id,
  cc.closure_code_id,

  NULLIF(TRIM(s."No_of_Reassignments"), '')::integer,

  NULLIF(TRIM(s."No_of_Related_Interactions"), '')::integer,
  TRIM(s."Related_Interaction") AS related_interaction_raw,

  NULLIF(TRIM(s."No_of_Related_Incidents"), '')::integer,

  NULLIF(TRIM(s."No_of_Related_Changes"), '')::integer,
  NULLIF(TRIM(s."Related_Change"), '') AS related_change_raw,

  CASE
    WHEN s."Open_Time" IS NULL OR TRIM(s."Open_Time") = '' THEN NULL
    WHEN position('/' in s."Open_Time") > 0 THEN to_timestamp(s."Open_Time", 'MM/DD/YYYY HH24:MI')
    WHEN position('-' in s."Open_Time") > 0 THEN to_timestamp(s."Open_Time", 'DD-MM-YYYY HH24:MI')
    ELSE NULL
  END AS open_time,

  CASE
    WHEN s."Reopen_Time" IS NULL OR TRIM(s."Reopen_Time") = '' THEN NULL
    WHEN position('/' in s."Reopen_Time") > 0 THEN to_timestamp(s."Reopen_Time", 'MM/DD/YYYY HH24:MI')
    WHEN position('-' in s."Reopen_Time") > 0 THEN to_timestamp(s."Reopen_Time", 'DD-MM-YYYY HH24:MI')
    ELSE NULL
  END AS reopen_time,

  CASE
    WHEN s."Resolved_Time" IS NULL OR TRIM(s."Resolved_Time") = '' THEN NULL
    WHEN position('/' in s."Resolved_Time") > 0 THEN to_timestamp(s."Resolved_Time", 'MM/DD/YYYY HH24:MI')
    WHEN position('-' in s."Resolved_Time") > 0 THEN to_timestamp(s."Resolved_Time", 'DD-MM-YYYY HH24:MI')
    ELSE NULL
  END AS resolved_time,

  CASE
    WHEN s."Close_Time" IS NULL OR TRIM(s."Close_Time") = '' THEN NULL
    WHEN position('/' in s."Close_Time") > 0 THEN to_timestamp(s."Close_Time", 'MM/DD/YYYY HH24:MI')
    WHEN position('-' in s."Close_Time") > 0 THEN to_timestamp(s."Close_Time", 'DD-MM-YYYY HH24:MI')
    ELSE NULL
  END AS close_time,

  CASE
    WHEN s."Handle_Time_hrs" IS NULL OR TRIM(s."Handle_Time_hrs") = '' THEN NULL
    ELSE (replace(s."Handle_Time_hrs", ',', ''))::numeric
  END AS handle_time_hours,

  NULLIF(TRIM(s."number_cnt"), '')::numeric,

  TRIM(s."Alert_Status") AS alert_status

FROM stg_itsm s

JOIN wbs w
  ON w.wbs_code = TRIM(s."WBS")

JOIN configuration_item ci
  ON ci.ci_code = TRIM(s."CI_Name")

JOIN incident_status_dim st
  ON st.status_name = TRIM(s."Status")

JOIN incident_category_dim cat
  ON cat.category_name = TRIM(s."Category")

JOIN impact_dim imp
  ON imp.impact_code = TRIM(s."Impact")

JOIN urgency_dim urg
  ON urg.urgency_code = TRIM(s."Urgency")

JOIN priority_dim pri
  ON pri.priority_code = COALESCE(NULLIF(TRIM(s."Priority"), ''), 'Unknown')

JOIN kb_article kb
  ON kb.kb_number = TRIM(s."KB_number")

JOIN closure_code_dim cc
  ON cc.closure_code_name = COALESCE(NULLIF(TRIM(s."Closure_Code"), ''), 'Unknown');

-- =========================
-- 4) Verificações rápidas
-- =========================
SELECT COUNT(*) AS incident_rows FROM incident; -- esperado: 46606
