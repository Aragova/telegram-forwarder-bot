from datetime import datetime, timezone
import asyncio
from app.repost_campaign_schedule_service import parse_campaign_schedule_input_to_utc, format_campaign_schedule_datetime, RepostCampaignScheduleService

class FR:
    def __init__(self): self.rows=[]; self.claim=[]
    def create_campaign_scheduled_launch(self, **kw): self.rows.append(kw); return 1
    def reset_stuck_campaign_scheduled_launches(self, **kw): return 0
    def claim_due_campaign_scheduled_launches(self, **kw): return self.claim
    def mark_campaign_scheduled_launch_failed(self, *a, **k): return True
    def mark_campaign_scheduled_launch_launched(self, *a, **k): return True

class RT:
    def build_campaign_launch_readiness(self, **kw): return {'can_launch':True,'saved_post_id':10,'show_seconds':3600}
    async def launch_campaign_now(self, **kw):
        class R: ok=True; extra={'campaign_run_id':55}; error_text=None
        return R()

def test_parse_campaign_schedule_input_to_utc():
    now=datetime(2026,5,8,10,0,tzinfo=timezone.utc)
    dt=parse_campaign_schedule_input_to_utc('09.05 18:00', now_utc=now)
    assert dt is not None

def test_format_campaign_schedule_datetime():
    dt=datetime(2026,5,9,15,0,tzinfo=timezone.utc)
    assert 'UTC+3' in format_campaign_schedule_datetime(dt)

def test_schedule_campaign_launch_saves_row_not_launch_now():
    repo=FR(); rt=RT(); svc=RepostCampaignScheduleService(repo=repo,campaign_runtime=rt)
    res=svc.schedule_campaign_launch(rule_id=1, scheduled_at_utc=datetime(2026,5,9,15,0,tzinfo=timezone.utc), created_by=7)
    assert res.ok and repo.rows
