from __future__ import annotations
import asyncio, logging, os
from datetime import datetime, timedelta, timezone
from app.repost_campaign_runtime_service import RepostCampaignActionResult

CAMPAIGN_SCHEDULE_TIMEZONE_OFFSET_MINUTES = 180
CAMPAIGN_SCHEDULE_TIMEZONE_LABEL = "UTC+3"
CAMPAIGN_SCHEDULE_LOOP_INTERVAL_SECONDS = 15
CAMPAIGN_SCHEDULE_STUCK_SECONDS = 300

def campaign_schedule_now_utc() -> datetime:
    return datetime.now(timezone.utc)

def parse_campaign_schedule_input_to_utc(text: str, *, now_utc: datetime | None = None, timezone_offset_minutes: int = 180) -> datetime | None:
    src=(text or "").strip(); now_utc=now_utc or campaign_schedule_now_utc(); local_now=now_utc+timedelta(minutes=timezone_offset_minutes)
    for fmt in ("%d.%m %H:%M","%d.%m.%Y %H:%M","%Y-%m-%d %H:%M"):
        try:
            dt=datetime.strptime(src,fmt)
            if fmt=="%d.%m %H:%M": dt=dt.replace(year=local_now.year)
            utc=(dt-timedelta(minutes=timezone_offset_minutes)).replace(tzinfo=timezone.utc)
            if utc < now_utc + timedelta(minutes=1): return None
            return utc
        except Exception:
            pass
    return None

def format_campaign_schedule_datetime(value, *, timezone_offset_minutes: int = 180, timezone_label: str = "UTC+3") -> str:
    if value is None: return "—"
    dt=value
    if isinstance(value,str): dt=datetime.fromisoformat(value.replace('Z','+00:00'))
    if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
    local=dt.astimezone(timezone.utc)+timedelta(minutes=timezone_offset_minutes)
    return f"{local.strftime('%d.%m %H:%M')} {timezone_label}"

class RepostCampaignScheduleService:
    def __init__(self, *, repo, campaign_runtime, logger_=None): self.repo=repo; self.campaign_runtime=campaign_runtime; self.logger=logger_ or logging.getLogger('forwarder')
    def build_schedule_readiness(self, *, rule_id:int, scheduled_at_utc:datetime)->dict:
        r=self.campaign_runtime.build_campaign_launch_readiness(rule_id=rule_id); r['scheduled_at']=scheduled_at_utc.isoformat(); r['scheduled_at_text']=format_campaign_schedule_datetime(scheduled_at_utc)
        show=int(r.get('show_seconds') or 0); r['expected_delete_at_text']=format_campaign_schedule_datetime(scheduled_at_utc+timedelta(seconds=show)) if show>0 else '—'; return r
    def schedule_campaign_launch(self, *, rule_id:int, scheduled_at_utc:datetime, created_by:int|None=None)->RepostCampaignActionResult:
        rd=self.build_schedule_readiness(rule_id=rule_id, scheduled_at_utc=scheduled_at_utc)
        if not rd.get('can_launch'): return RepostCampaignActionResult(ok=False, action='schedule_campaign_launch', rule_id=rule_id, error_text='Кампания не готова к запуску', extra={'launch_readiness':rd})
        sid=self.repo.create_campaign_scheduled_launch(rule_id=rule_id,saved_post_id=int(rd.get('saved_post_id')),show_seconds=int(rd.get('show_seconds') or 0),scheduled_at=scheduled_at_utc.isoformat(),timezone_offset_minutes=CAMPAIGN_SCHEDULE_TIMEZONE_OFFSET_MINUTES,timezone_label=CAMPAIGN_SCHEDULE_TIMEZONE_LABEL,created_by=created_by,preview=rd)
        return RepostCampaignActionResult(ok=bool(sid), action='schedule_campaign_launch', rule_id=rule_id, saved_post_id=int(rd.get('saved_post_id')), extra={'scheduled_launch_id':sid,'scheduled_at':scheduled_at_utc.isoformat(),'scheduled_at_text':format_campaign_schedule_datetime(scheduled_at_utc),'expected_delete_at_text':rd.get('expected_delete_at_text')})
    def cancel_scheduled_launch(self, *, scheduled_launch_id:int, cancelled_by:int|None=None)->RepostCampaignActionResult:
        row=self.repo.get_campaign_scheduled_launch(scheduled_launch_id)
        if not row: return RepostCampaignActionResult(ok=False, action='cancel_scheduled_launch', rule_id=0, error_text='Запланированный запуск не найден')
        if row.get('status')!='scheduled': return RepostCampaignActionResult(ok=False, action='cancel_scheduled_launch', rule_id=int(row.get('rule_id') or 0), error_text='Запуск уже нельзя отменить')
        ok=self.repo.cancel_campaign_scheduled_launch(scheduled_launch_id,cancelled_by=cancelled_by)
        return RepostCampaignActionResult(ok=ok, action='cancel_scheduled_launch', rule_id=int(row.get('rule_id') or 0))
    async def process_due_scheduled_launches(self, *, worker_id:str, limit:int=5)->dict:
        self.repo.reset_stuck_campaign_scheduled_launches(stuck_seconds=CAMPAIGN_SCHEDULE_STUCK_SECONDS)
        claimed=self.repo.claim_due_campaign_scheduled_launches(now_iso=campaign_schedule_now_utc().isoformat(),worker_id=worker_id,limit=limit)
        for row in claimed:
            rid=int(row['rule_id']); sid=int(row['id']); created_by=row.get('created_by')
            rd=self.campaign_runtime.build_campaign_launch_readiness(rule_id=rid)
            if (not rd.get('can_launch')) or rd.get('active_placement') or int(rd.get('delete_failed') or 0)>0:
                self.repo.mark_campaign_scheduled_launch_failed(sid,error_text='Кампания не готова к запуску в момент старта'); continue
            res=await self.campaign_runtime.launch_campaign_now(rule_id=rid, admin_id=created_by, run_type='scheduled')
            run_id=((res.extra or {}).get('campaign_run_id') if res else None)
            if res and res.ok and run_id: self.repo.mark_campaign_scheduled_launch_launched(sid,campaign_run_id=int(run_id))
            else: self.repo.mark_campaign_scheduled_launch_failed(sid,error_text=res.error_text if res else 'Ошибка запуска')
        return {'claimed':len(claimed)}

async def run_repost_campaign_scheduled_launch_loop(*, runtime: RepostCampaignScheduleService, stop_event: asyncio.Event | None = None, interval_seconds: int = CAMPAIGN_SCHEDULE_LOOP_INTERVAL_SECONDS, worker_id: str | None = None):
    logger=runtime.logger; wid=worker_id or f"{os.uname().nodename}:{os.getpid()}"; logger.info('REPOST_CAMPAIGN_SCHEDULE_LOOP_STARTED | worker_id=%s', wid)
    while not (stop_event and stop_event.is_set()):
        try: await runtime.process_due_scheduled_launches(worker_id=wid)
        except Exception: logger.exception('REPOST_CAMPAIGN_SCHEDULE_LOOP_FAILED | worker_id=%s', wid)
        await asyncio.sleep(interval_seconds)
