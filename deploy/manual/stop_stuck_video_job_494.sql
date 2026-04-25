BEGIN;

-- 1) Останавливаем зависшую задачу #494 только из активных статусов.
UPDATE jobs
SET
    status = 'failed',
    error_text = 'Остановлено вручную: битый mp4, moov atom not found',
    lease_until = NULL,
    locked_by = NULL,
    updated_at = NOW()
WHERE id = 494
  AND status IN ('pending', 'leased', 'processing', 'retry');

-- 2) Помечаем доставку #471482 как faulty.
UPDATE deliveries
SET
    status = 'faulty',
    error_text = 'Остановлено вручную: битый mp4, moov atom not found',
    updated_at = NOW()
WHERE id = 471482;

COMMIT;

-- 3) Контрольный вывод.
SELECT id, job_type, status, attempts, max_attempts, error_text
FROM jobs
WHERE id = 494;

SELECT id, rule_id, status, error_text
FROM deliveries
WHERE id = 471482;
