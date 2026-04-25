BEGIN;

-- 1) Останавливаем все активные задачи по delivery_id=471482 (ищем в payload_json).
UPDATE jobs
SET
    status = 'failed',
    error_text = 'Остановлено вручную: битый MP4, moov atom not found',
    lease_until = NULL,
    locked_by = NULL,
    updated_at = NOW()
WHERE status IN ('pending', 'leased', 'processing', 'retry')
  AND (payload_json ->> 'delivery_id')::BIGINT = 471482;

-- 2) Помечаем delivery как faulty.
UPDATE deliveries
SET
    status = 'faulty',
    error_text = 'Остановлено вручную: битый MP4, moov atom not found',
    updated_at = NOW()
WHERE id = 471482;

COMMIT;

-- 3) Контрольные выборки.
SELECT id, job_type, status, attempts, max_attempts, error_text, payload_json
FROM jobs
WHERE (payload_json ->> 'delivery_id')::BIGINT = 471482
ORDER BY id DESC;

SELECT id, rule_id, status, error_text
FROM deliveries
WHERE id = 471482;
