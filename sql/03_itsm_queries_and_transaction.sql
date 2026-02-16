-- =============================================================
-- 03_itsm_queries_and_transaction.sql
-- Projeto ITSM (PostgreSQL) — CRUD (JOINs) + Transação multi-tabela
-- =============================================================

SET search_path = public;

-- =========================
-- 1) CRUD — 4 queries relevantes (preferencialmente com JOINs)
-- =========================

-- 1.1 CREATE (Inserir novo incidente) — usa JOINs para obter FKs e matriz (impact+urgency -> priority)
-- Nota: os valores usados nos JOINs devem existir nas dimensões (respeitar maiúsculas/minúsculas).
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
  'IM9999999' AS incident_id,
  w.wbs_id,
  ci.ci_id,
  st.status_id,
  cat.category_id,
  imp.impact_id,
  urg.urgency_id,
  pm.priority_id,
  kb.kb_id,
  cc.closure_code_id,
  0 AS no_of_reassignments,
  1 AS no_of_related_interactions,
  'SD9999999' AS related_interaction_raw,
  NULL AS no_of_related_incidents,
  NULL AS no_of_related_changes,
  NULL AS related_change_raw,
  now() AS open_time,
  NULL AS reopen_time,
  NULL AS resolved_time,
  NULL AS close_time,
  NULL AS handle_time_hours,
  0.0::numeric AS number_cnt,
  'closed' AS alert_status
FROM wbs w
JOIN configuration_item ci ON ci.ci_code = 'WBA000124'
JOIN incident_status_dim st ON st.status_name = 'Work in progress'
JOIN incident_category_dim cat ON cat.category_name = 'incident'
JOIN impact_dim imp ON imp.impact_code = '2'
JOIN urgency_dim urg ON urg.urgency_code = '2'
JOIN priority_matrix pm ON pm.impact_id = imp.impact_id AND pm.urgency_id = urg.urgency_id
JOIN kb_article kb ON kb.kb_number = 'KM0000611'
JOIN closure_code_dim cc ON cc.closure_code_name = 'Unknown'
WHERE w.wbs_code = 'WBS000088';


-- 1.2 READ (Listagem detalhada) — JOINs + filtros úteis
SELECT
  i.incident_id,
  w.wbs_code,
  ci.ci_code AS ci_name,
  ccat.ci_cat_name,
  csub.ci_subcat_name,
  st.status_name,
  ic.category_name AS ticket_category,
  imp.impact_code,
  urg.urgency_code,
  pri.priority_code,
  kb.kb_number,
  cc.closure_code_name,
  i.open_time,
  i.resolved_time,
  i.close_time,
  i.handle_time_hours
FROM incident i
JOIN wbs w ON w.wbs_id = i.wbs_id
JOIN configuration_item ci ON ci.ci_id = i.ci_id
JOIN ci_category_dim ccat ON ccat.ci_cat_id = ci.ci_cat_id
JOIN ci_subcategory_dim csub ON csub.ci_subcat_id = ci.ci_subcat_id
JOIN incident_status_dim st ON st.status_id = i.status_id
JOIN incident_category_dim ic ON ic.category_id = i.category_id
JOIN impact_dim imp ON imp.impact_id = i.impact_id
JOIN urgency_dim urg ON urg.urgency_id = i.urgency_id
JOIN priority_dim pri ON pri.priority_id = i.priority_id
JOIN kb_article kb ON kb.kb_id = i.kb_id
JOIN closure_code_dim cc ON cc.closure_code_id = i.closure_code_id
WHERE st.status_name = 'Work in progress'
  AND pri.priority_code IN ('1', '2')
ORDER BY i.open_time DESC
LIMIT 50;


-- 1.3 UPDATE (Alterar estado + tempos + closure) — com JOINs para resolver IDs
-- Nota: usar closure_code_name existente no dataset (ex.: Software/Hardware/Other/Unknown)
UPDATE incident i
SET
  status_id = st.status_id,
  closure_code_id = cc.closure_code_id,
  resolved_time = COALESCE(i.resolved_time, now()),
  close_time = COALESCE(i.close_time, now())
FROM incident_status_dim st
JOIN closure_code_dim cc ON cc.closure_code_name = 'Software'
WHERE i.incident_id = 'IM0000005'
  AND st.status_name = 'Closed';


-- 1.4 DELETE (Eliminar incidente) — com JOIN para garantir critério
DELETE FROM incident i
USING incident_status_dim st
WHERE i.status_id = st.status_id
  AND i.incident_id = 'IM9999999'
  AND st.status_name = 'Work in progress';


-- =========================
-- 2) Transação multi-tabela: "Fechar Incidente"
-- Atualiza incident + insere histórico + atualiza CI.last_closed_incident_id
-- Regras:
-- - Só registar histórico se houver mudança real de estado (old_status_id <> new_status_id)
-- - Só atualizar CI se o incidente tiver sido efetivamente fechado
-- =========================

BEGIN;

WITH target AS (
  SELECT
    i.incident_id,
    i.status_id AS old_status_id,
    i.ci_id
  FROM incident i
  WHERE i.incident_id = 'IM0003614'     -- exemplo (trocar conforme teste)
  FOR UPDATE
),
dims AS (
  SELECT
    st_closed.status_id AS closed_status_id,
    cc.closure_code_id  AS closure_code_id
  FROM incident_status_dim st_closed
  JOIN closure_code_dim cc
    ON cc.closure_code_name = 'Hardware' -- exemplo (usar valor existente)
  WHERE st_closed.status_name = 'Closed'
),
upd AS (
  UPDATE incident i
  SET
    status_id = (SELECT closed_status_id FROM dims),
    closure_code_id = (SELECT closure_code_id FROM dims),
    resolved_time = COALESCE(i.resolved_time, now()),
    close_time = COALESCE(i.close_time, now())
  WHERE i.incident_id = (SELECT incident_id FROM target)
    AND (SELECT old_status_id FROM target) <> (SELECT closed_status_id FROM dims)
  RETURNING
    i.incident_id,
    (SELECT old_status_id FROM target) AS old_status_id,
    i.status_id AS new_status_id,
    i.ci_id
),
ins_hist AS (
  INSERT INTO incident_status_history (incident_id, old_status_id, new_status_id, changed_at)
  SELECT incident_id, old_status_id, new_status_id, now()
  FROM upd
  RETURNING incident_id
)
UPDATE configuration_item ci
SET last_closed_incident_id = u.incident_id
FROM upd u
WHERE ci.ci_id = u.ci_id;

COMMIT;


-- =========================
-- 3) Testes rápidos (após transação)
-- =========================

-- Teste 1: Verificar incidente (com nomes legíveis)
SELECT
  i.incident_id,
  st.status_name,
  cc.closure_code_name,
  i.resolved_time,
  i.close_time
FROM incident i
JOIN incident_status_dim st ON st.status_id = i.status_id
LEFT JOIN closure_code_dim cc ON cc.closure_code_id = i.closure_code_id
WHERE i.incident_id = 'IM0003614';

-- Teste 2: Verificar histórico (últimas alterações)
SELECT
  h.history_id,
  h.incident_id,
  h.old_status_id,
  h.new_status_id,
  h.changed_at
FROM incident_status_history h
WHERE h.incident_id = 'IM0003614'
ORDER BY h.changed_at DESC
LIMIT 10;

-- Teste 2b: Histórico com nomes dos estados
SELECT
  h.history_id,
  h.incident_id,
  os.status_name AS old_status,
  ns.status_name AS new_status,
  h.changed_at
FROM incident_status_history h
JOIN incident_status_dim os ON os.status_id = h.old_status_id
JOIN incident_status_dim ns ON ns.status_id = h.new_status_id
WHERE h.incident_id = 'IM0003614'
ORDER BY h.changed_at DESC
LIMIT 10;

-- Teste 3: Verificar CI atualizado
SELECT
  ci.ci_id,
  ci.ci_code,
  ci.last_closed_incident_id
FROM configuration_item ci
WHERE ci.ci_id = (SELECT i.ci_id FROM incident i WHERE i.incident_id = 'IM0003614');
