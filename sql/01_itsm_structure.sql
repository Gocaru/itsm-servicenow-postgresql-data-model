-- =============================================================
-- 01_itsm_structure.sql
-- Projeto ITSM (PostgreSQL) — Estrutura 3NF (DDL)
-- Inclui: Tabelas, chaves, restrições, índices e objetos de suporte
-- =============================================================

-- Recomenda-se executar este ficheiro na BD alvo (ex.: itsm).
SET search_path = public;

-- Para reexecução (ambiente de testes), pode descomentar os DROPs abaixo.
-- ATENÇÃO: isto apaga dados.
-- DROP TABLE IF EXISTS incident_status_history CASCADE;
-- DROP TABLE IF EXISTS incident CASCADE;
-- DROP TABLE IF EXISTS priority_matrix CASCADE;
-- DROP TABLE IF EXISTS configuration_item CASCADE;
-- DROP TABLE IF EXISTS wbs CASCADE;
-- DROP TABLE IF EXISTS kb_article CASCADE;
-- DROP TABLE IF EXISTS closure_code_dim CASCADE;
-- DROP TABLE IF EXISTS incident_category_dim CASCADE;
-- DROP TABLE IF EXISTS incident_status_dim CASCADE;
-- DROP TABLE IF EXISTS priority_dim CASCADE;
-- DROP TABLE IF EXISTS urgency_dim CASCADE;
-- DROP TABLE IF EXISTS impact_dim CASCADE;
-- DROP TABLE IF EXISTS ci_subcategory_dim CASCADE;
-- DROP TABLE IF EXISTS ci_category_dim CASCADE;
-- DROP TABLE IF EXISTS stg_itsm CASCADE;

-- =========================
-- Dimensão WBS
-- =========================
CREATE TABLE IF NOT EXISTS wbs (
  wbs_id   bigserial PRIMARY KEY,
  wbs_code text NOT NULL UNIQUE
);

-- =========================
-- Dimensões de CI
-- =========================
CREATE TABLE IF NOT EXISTS ci_category_dim (
  ci_cat_id   bigserial PRIMARY KEY,
  ci_cat_name text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS ci_subcategory_dim (
  ci_subcat_id   bigserial PRIMARY KEY,
  ci_subcat_name text NOT NULL UNIQUE
);

-- =========================
-- Configuration Item (CI)
-- =========================
CREATE TABLE IF NOT EXISTS configuration_item (
  ci_id        bigserial PRIMARY KEY,
  ci_code      text NOT NULL UNIQUE,     -- CI_Name no CSV
  ci_cat_id    bigint NOT NULL REFERENCES ci_category_dim(ci_cat_id),
  ci_subcat_id bigint NOT NULL REFERENCES ci_subcategory_dim(ci_subcat_id),

  -- Campo de suporte à transação multi-tabela:
  last_closed_incident_id text NULL
);

-- =========================
-- Dimensões ITSM
-- =========================
CREATE TABLE IF NOT EXISTS impact_dim (
  impact_id   bigserial PRIMARY KEY,
  impact_code text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS urgency_dim (
  urgency_id   bigserial PRIMARY KEY,
  urgency_code text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS priority_dim (
  priority_id   bigserial PRIMARY KEY,
  priority_code text NOT NULL UNIQUE,
  CONSTRAINT chk_priority_code_not_empty CHECK (length(trim(priority_code)) > 0)
);

-- Matriz: (impact, urgency) -> priority
CREATE TABLE IF NOT EXISTS priority_matrix (
  impact_id   bigint NOT NULL REFERENCES impact_dim(impact_id),
  urgency_id  bigint NOT NULL REFERENCES urgency_dim(urgency_id),
  priority_id bigint NOT NULL REFERENCES priority_dim(priority_id),
  PRIMARY KEY (impact_id, urgency_id),
  UNIQUE (impact_id, urgency_id, priority_id)
);

-- Outras dimensões do incidente
CREATE TABLE IF NOT EXISTS incident_status_dim (
  status_id   bigserial PRIMARY KEY,
  status_name text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS incident_category_dim (
  category_id   bigserial PRIMARY KEY,
  category_name text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS closure_code_dim (
  closure_code_id   bigserial PRIMARY KEY,
  closure_code_name text NOT NULL UNIQUE,
  CONSTRAINT chk_closure_code_name_not_empty CHECK (length(trim(closure_code_name)) > 0)
);

CREATE TABLE IF NOT EXISTS kb_article (
  kb_id     bigserial PRIMARY KEY,
  kb_number text NOT NULL UNIQUE,
  CONSTRAINT chk_kb_number_not_empty CHECK (length(trim(kb_number)) > 0)
);

-- =========================
-- Incidente (tabela central)
-- =========================
CREATE TABLE IF NOT EXISTS incident (
  incident_id text PRIMARY KEY, -- Incident_ID (IM000...)

  wbs_id  bigint NOT NULL REFERENCES wbs(wbs_id),
  ci_id   bigint NOT NULL REFERENCES configuration_item(ci_id),

  status_id   bigint NOT NULL REFERENCES incident_status_dim(status_id),
  category_id bigint NOT NULL REFERENCES incident_category_dim(category_id),

  impact_id   bigint NOT NULL REFERENCES impact_dim(impact_id),
  urgency_id  bigint NOT NULL REFERENCES urgency_dim(urgency_id),
  priority_id bigint NOT NULL REFERENCES priority_dim(priority_id),

  -- validação ITSM: snapshot coerente com matriz
  CONSTRAINT fk_priority_matrix
    FOREIGN KEY (impact_id, urgency_id, priority_id)
    REFERENCES priority_matrix(impact_id, urgency_id, priority_id),

  kb_id           bigint NOT NULL REFERENCES kb_article(kb_id),
  closure_code_id bigint NOT NULL REFERENCES closure_code_dim(closure_code_id),

  no_of_reassignments        integer NULL,
  no_of_related_interactions integer NULL,
  related_interaction_raw    text NOT NULL,
  no_of_related_incidents    integer NULL,
  no_of_related_changes      integer NULL,
  related_change_raw         text NULL,

  open_time     timestamp NULL,
  reopen_time   timestamp NULL,
  resolved_time timestamp NULL,
  close_time    timestamp NULL,

  handle_time_hours numeric NULL,
  number_cnt       numeric NULL,

  alert_status text NOT NULL
);

-- FK: CI -> último incidente fechado (após existir incident)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints
    WHERE constraint_name = 'fk_ci_last_closed_incident'
      AND table_name = 'configuration_item'
      AND table_schema = 'public'
  ) THEN
    ALTER TABLE configuration_item
      ADD CONSTRAINT fk_ci_last_closed_incident
      FOREIGN KEY (last_closed_incident_id)
      REFERENCES incident(incident_id);
  END IF;
END $$;

-- =========================
-- Auditoria: Histórico de estados
-- =========================
CREATE TABLE IF NOT EXISTS incident_status_history (
  history_id    bigserial PRIMARY KEY,
  incident_id   text NOT NULL REFERENCES incident(incident_id),
  old_status_id bigint NOT NULL REFERENCES incident_status_dim(status_id),
  new_status_id bigint NOT NULL REFERENCES incident_status_dim(status_id),
  changed_at    timestamp NOT NULL DEFAULT now(),

  CONSTRAINT chk_status_change CHECK (old_status_id <> new_status_id)
);

-- =========================
-- Índices (casos de uso)
-- =========================
CREATE INDEX IF NOT EXISTS ix_incident_status
  ON incident (status_id);

CREATE INDEX IF NOT EXISTS ix_incident_priority
  ON incident (priority_id);

CREATE INDEX IF NOT EXISTS ix_incident_impact_urgency
  ON incident (impact_id, urgency_id);

CREATE INDEX IF NOT EXISTS ix_incident_ci
  ON incident (ci_id);

CREATE INDEX IF NOT EXISTS ix_incident_wbs
  ON incident (wbs_id);

CREATE INDEX IF NOT EXISTS ix_incident_open_time
  ON incident (open_time);

CREATE INDEX IF NOT EXISTS ix_incident_kb
  ON incident (kb_id);

CREATE INDEX IF NOT EXISTS ix_hist_incident
  ON incident_status_history (incident_id);
