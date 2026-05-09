from app.postgres_repository import PostgresRepository
from contextlib import contextmanager

class _C:
    def __init__(self,row=None,rows=None,rowcount=1): self.row=row; self.rows=rows or []; self.rowcount=rowcount; self.last_sql=''
    def __enter__(self): return self
    def __exit__(self,*a): return False
    def execute(self,*a,**k): self.last_sql=a[0]
    def fetchone(self): return self.row
    def fetchall(self): return self.rows
class _Conn:
    def __init__(self,c): self.c=c
    def cursor(self): return self.c
    def commit(self): pass

def _repo(row=None,rows=None,rowcount=1):
    repo=PostgresRepository(); c=_C(row,rows,rowcount)
    @contextmanager
    def _connect(): yield _Conn(c)
    repo.connect=_connect
    return repo,c

def test_methods_exist():
    repo=PostgresRepository()
    for n in ["create_campaign_scheduled_post_draft","update_campaign_scheduled_post","get_campaign_scheduled_post","list_campaign_scheduled_posts","list_campaign_scheduled_posts_for_tenant","replace_campaign_scheduled_post_targets","list_campaign_scheduled_post_targets","update_campaign_scheduled_post_target_check_result","log_campaign_scheduled_post_check","list_campaign_scheduled_post_checks","log_campaign_scheduled_post_event","list_campaign_scheduled_post_events","schedule_campaign_scheduled_post","cancel_campaign_scheduled_post","mark_campaign_scheduled_post_launched","mark_campaign_scheduled_post_failed","delay_campaign_scheduled_post_retry","claim_due_campaign_scheduled_posts","reset_stuck_campaign_scheduled_posts"]:
        assert hasattr(repo,n)

def test_create_and_get_campaign_scheduled_post_draft():
    repo,_=_repo(row={"id":11}); assert repo.create_campaign_scheduled_post_draft(rule_id=1,tenant_id=1,created_by=2)==11

def test_claim_due_campaign_scheduled_posts_uses_skip_locked_and_run_null_filter():
    repo,c=_repo(rows=[]); repo.claim_due_campaign_scheduled_posts(now_iso="2026-05-09T00:00:00+00:00",worker_id="w",limit=2)
    assert "FOR UPDATE SKIP LOCKED" in c.last_sql and "campaign_run_id IS NULL" in c.last_sql

def test_reset_stuck_campaign_scheduled_posts_does_not_reset_with_campaign_run():
    repo,c=_repo(rowcount=0); repo.reset_stuck_campaign_scheduled_posts(stuck_seconds=300)
    assert "campaign_run_id IS NULL" in c.last_sql

def test_create_campaign_run_supports_scheduled_post_id():
    repo,_=_repo(row={"id":5}); run_id=repo.create_campaign_run(rule_id=1,saved_post_id=2,run_type="scheduled",status="created",show_seconds=1,started_by=1,scheduled_post_id=77)
    assert run_id==5
