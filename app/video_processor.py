import os
import shutil
import time
import asyncio
import json
from pathlib import Path
import logging
from .config import settings as config
from aiogram import Bot
from aiogram.types import FSInputFile, MessageEntity
from telethon import types as tl_types

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VideoProcessor:
    def __init__(self, bot=None, telethon_client=None):
        self.temp_dir = str(config.temp_dir)
        self.intros_dir = str(config.intros_dir)
        self.min_free_space_gb = config.min_free_space_gb
        self.max_temp_size_gb = config.max_temp_size_gb

        self.bot = bot or Bot(
            token=config.bot_token,
            base_url=f"{config.bot_api_base}/bot",
        )

        self.telethon = telethon_client

        os.makedirs(self.temp_dir, exist_ok=True)
        os.makedirs(self.intros_dir, exist_ok=True)

        # Целевые параметры из конфига
        self.target_width = config.target_width
        self.target_height = config.target_height
        self.target_fps = min(config.target_fps, 60)
        self.target_sample_rate = config.target_sample_rate
        self.target_channels = config.target_channels
        self.target_pix_fmt = config.target_pix_fmt

        # Оптимальные настройки FFmpeg для слабого VPS
        self.video_codec = "libx264"
        self.audio_codec = "aac"
        self.audio_bitrate = "96k"
        self.crf = 26
        self.preset = "veryfast"

        # Лимиты битрейта - УБИРАЕМ для скорости
        self.max_bitrate = None
        self.bufsize = None

        # Ограничение параллельности для слабого VPS - 1 задача
        self.semaphore = asyncio.Semaphore(1)
        self.cleanup_tasks = []
        self.video_info_cache = {}

        # Лимиты кэша
        self.MAX_CACHE_SIZE = 100
        self.CACHE_TTL = 3600

    def _build_telethon_caption_entities_from_json(self, caption_entities_json):
        if not caption_entities_json:
            return None

        try:
            if isinstance(caption_entities_json, str):
                entities_data = json.loads(caption_entities_json)
            elif isinstance(caption_entities_json, dict):
                entities_data = [caption_entities_json]
            elif isinstance(caption_entities_json, list):
                entities_data = caption_entities_json
            else:
                logger.warning("❌ Неподдерживаемый тип caption_entities_json: %s", type(caption_entities_json))
                return None

            if isinstance(entities_data, dict):
                entities_data = [entities_data]

            if not isinstance(entities_data, list):
                logger.warning("❌ caption_entities_json после нормализации не является списком")
                return None

            result = []

            for e in entities_data:
                if not isinstance(e, dict):
                    continue

                entity_type = str(e.get("type") or "").strip().lower()
                offset = int(e.get("offset", 0) or 0)
                length = int(e.get("length", 0) or 0)

                if length <= 0:
                    continue

                if entity_type == "bold":
                    result.append(tl_types.MessageEntityBold(offset=offset, length=length))

                elif entity_type == "italic":
                    result.append(tl_types.MessageEntityItalic(offset=offset, length=length))

                elif entity_type == "underline":
                    result.append(tl_types.MessageEntityUnderline(offset=offset, length=length))

                elif entity_type in ("strikethrough", "strike"):
                    result.append(tl_types.MessageEntityStrike(offset=offset, length=length))

                elif entity_type == "spoiler":
                    result.append(tl_types.MessageEntitySpoiler(offset=offset, length=length))

                elif entity_type == "code":
                    result.append(tl_types.MessageEntityCode(offset=offset, length=length))

                elif entity_type == "pre":
                    result.append(
                        tl_types.MessageEntityPre(
                            offset=offset,
                            length=length,
                            language=str(e.get("language") or ""),
                        )
                    )

                elif entity_type == "text_link":
                    url = str(e.get("url") or "").strip()
                    if url:
                        result.append(
                            tl_types.MessageEntityTextUrl(
                                offset=offset,
                                length=length,
                                url=url,
                            )
                        )

                elif entity_type == "url":
                    result.append(tl_types.MessageEntityUrl(offset=offset, length=length))

                elif entity_type == "mention":
                    result.append(tl_types.MessageEntityMention(offset=offset, length=length))

                elif entity_type == "email":
                    result.append(tl_types.MessageEntityEmail(offset=offset, length=length))

                elif entity_type == "phone":
                    result.append(tl_types.MessageEntityPhone(offset=offset, length=length))

                elif entity_type == "hashtag":
                    result.append(tl_types.MessageEntityHashtag(offset=offset, length=length))

                elif entity_type == "cashtag":
                    result.append(tl_types.MessageEntityCashtag(offset=offset, length=length))

                elif entity_type == "bot_command":
                    result.append(tl_types.MessageEntityBotCommand(offset=offset, length=length))

                elif entity_type == "blockquote":
                    result.append(tl_types.MessageEntityBlockquote(offset=offset, length=length))

                elif entity_type == "custom_emoji":
                    custom_emoji_id = e.get("custom_emoji_id")
                    if custom_emoji_id:
                        result.append(
                            tl_types.MessageEntityCustomEmoji(
                                offset=offset,
                                length=length,
                                document_id=int(custom_emoji_id),
                            )
                        )

            return result or None

        except Exception as exc:
            logger.error("❌ Ошибка сборки telethon caption entities: %s", exc)
            return None

    async def _send_video_via_telethon(
        self,
        destination_channel,
        target_thread_id,
        video_path,
        thumbnail_path,
        caption,
        caption_entities_json=None,
    ):
        if not self.telethon:
            raise RuntimeError("Telethon-клиент не передан в VideoProcessor")

        entity = int(destination_channel) if str(destination_channel).lstrip("-").isdigit() else destination_channel
        formatting_entities = self._build_telethon_caption_entities_from_json(caption_entities_json)

        video_info = await self.get_video_info(video_path, use_cache=False)
        if not video_info:
            raise RuntimeError(f"Не удалось получить video_info для Telethon-отправки: {video_path}")

        width = int(video_info.get("width") or 0)
        height = int(video_info.get("height") or 0)
        duration = max(1, int(round(float(video_info.get("duration") or 0))))

        attributes = [
            tl_types.DocumentAttributeVideo(
                duration=duration,
                w=width,
                h=height,
                supports_streaming=True,
                round_message=False,
            )
        ]

        send_kwargs = {
            "entity": entity,
            "file": str(video_path),
            "caption": caption or "",
            "formatting_entities": formatting_entities,
            "attributes": attributes,
            "supports_streaming": True,
            "link_preview": False,
        }

        if thumbnail_path and os.path.isfile(thumbnail_path):
            send_kwargs["thumb"] = str(thumbnail_path)

        if target_thread_id is not None:
            send_kwargs["comment_to"] = int(target_thread_id)

        logger.info(
            "TELETHON_VIDEO_SEND | START | target=%s | thread=%s | file=%s | duration=%s | size=%sx%s | thumb=%s | entities=%s",
            destination_channel,
            target_thread_id,
            os.path.basename(str(video_path)),
            duration,
            width,
            height,
            bool(thumbnail_path and os.path.isfile(thumbnail_path)),
            len(formatting_entities or []),
        )

        sent_msg = await self.telethon.send_file(**send_kwargs)

        if isinstance(sent_msg, list):
            sent_msg = sent_msg[0] if sent_msg else None

        logger.info(
            "TELETHON_VIDEO_SEND | OK | target=%s | thread=%s | message_id=%s | duration=%s | size=%sx%s",
            destination_channel,
            target_thread_id,
            getattr(sent_msg, "id", None),
            duration,
            width,
            height,
        )

        return sent_msg

    # ==================== UTILS ====================

    def _get_free_space_gb(self):
        try:
            total, used, free = shutil.disk_usage(self.temp_dir)
            return free // (2 ** 30)
        except:
            return 0

    def _cleanup_cache(self):
        now = time.time()
        expired = [k for k, (v, ts) in self.video_info_cache.items() if now - ts > self.CACHE_TTL]
        for k in expired:
            del self.video_info_cache[k]
        
        if len(self.video_info_cache) > self.MAX_CACHE_SIZE:
            keys_to_remove = list(self.video_info_cache.keys())[:self.MAX_CACHE_SIZE // 2]
            for key in keys_to_remove:
                del self.video_info_cache[key]
            logger.debug(f"🧹 Очищено {len(keys_to_remove)} записей из кэша")

    def _validate_sample_rate(self, sample_rate):
        try:
            sr = int(sample_rate)
            if sr in [44100, 48000, 96000]:
                return sr
            return self.target_sample_rate
        except (ValueError, TypeError):
            return self.target_sample_rate

    def _validate_channels(self, channels):
        try:
            ch = int(channels)
            return min(max(ch, 1), self.target_channels)
        except (ValueError, TypeError):
            return self.target_channels

    def _get_rotate_filter(self, video_stream):
        rotate = None
        if 'tags' in video_stream:
            rotate = video_stream['tags'].get('rotate')
        if not rotate and 'side_data_list' in video_stream:
            for side_data in video_stream['side_data_list']:
                if side_data.get('side_data_type') == 'Display Matrix':
                    rotate = side_data.get('rotation', 0)
        if rotate:
            rotate = int(rotate) % 360
            if rotate == 90:
                return "transpose=1"
            elif rotate == 180:
                return "transpose=1,transpose=1"
            elif rotate == 270:
                return "transpose=2"
        return None

    def _map_codec_name(self, codec_name):
        codec_map = {
            'h264': 'libx264',
            'hevc': 'libx265',
            'h265': 'libx265',
            'mpeg4': 'mpeg4',
            'vp9': 'libvpx-vp9',
            'vp8': 'libvpx',
            'av1': 'libaom-av1'
        }
        return codec_map.get(codec_name, 'libx264')

    def _normalize_fps(self, fps):
        try:
            fps_float = float(fps)
            if abs(fps_float - 23.976) < 0.1 or abs(fps_float - 24) < 0.1:
                return 24
            elif abs(fps_float - 29.97) < 0.1 or abs(fps_float - 30) < 0.1:
                return 30
            elif abs(fps_float - 59.94) < 0.1 or abs(fps_float - 60) < 0.1:
                return 60
            else:
                return min(max(round(fps_float), 15), 60)
        except (ValueError, TypeError):
            return 30

    # ==================== PROFILE ====================

    def _get_processing_profile(self, video_info):
        width = video_info['width']
        height = video_info['height']
        fps_normalized = video_info['fps_normalized']

        is_vertical = height > width

        # Вертикальные видео: отдельный вертикальный профиль
        if is_vertical:
            target_width = 720
            target_height = 1280

            return {
                "target_width": target_width,
                "target_height": target_height,
                "target_fps": min(self.target_fps, 30, fps_normalized),
                "downscale": (width > target_width or height > target_height),
                "is_vertical": True,
                "reason": f"Вертикальное видео -> профиль {target_width}x{target_height}/{min(self.target_fps, 30, fps_normalized)}fps"
            }

        # Горизонтальные и квадратные: старая логика
        if width >= 1920 or height >= 1080:
            return {
                "target_width": self.target_width,
                "target_height": self.target_height,
                "target_fps": min(self.target_fps, 30),
                "downscale": True,
                "is_vertical": False,
                "reason": f"Видео 1080p+ -> принудительно {self.target_width}x{self.target_height}/{min(self.target_fps, 30)}fps"
            }

        return {
            "target_width": width,
            "target_height": height,
            "target_fps": min(fps_normalized, self.target_fps),
            "downscale": False,
            "is_vertical": False,
            "reason": f"Горизонтальное видео ниже 1080p -> оставляем разрешение, fps ≤ {self.target_fps}"
        }
    # ==================== VIDEO FILTER BUILDER ====================

    def _build_video_filter(self, width, height, fps, rotate_filter=None, 
                            original_width=None, original_height=None, 
                            original_fps=None, original_pix_fmt=None):
        filters = []
        
        if rotate_filter:
            filters.append(rotate_filter)
        
        if original_width != width or original_height != height:
            filters.append(f"scale={width}:{height}:force_original_aspect_ratio=decrease")
            filters.append(f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2")
        
        filters.append("setsar=1")
        
        if original_fps is None or abs(original_fps - fps) > 0.1:
            filters.append(f"fps={fps}")
        
        if original_pix_fmt is None or original_pix_fmt != 'yuv420p':
            filters.append("format=yuv420p")
        
        return ",".join(filters) if filters else "null"

    def _build_video_encode_args(self, preset=None, crf=None):
        args = [
            "-c:v", self.video_codec,
            "-preset", preset or self.preset,
            "-crf", str(crf or self.crf)
        ]
        return args

    def _build_audio_encode_args(self, has_audio, audio_info=None):
        if not has_audio:
            return ["-an"]
        
        sample_rate = self.target_sample_rate
        channels = self.target_channels
        
        if audio_info:
            sample_rate = self._validate_sample_rate(audio_info.get('audio_sample_rate', sample_rate))
            channels = self._validate_channels(audio_info.get('audio_channels', channels))
        
        return [
            "-c:a", self.audio_codec,
            "-b:a", self.audio_bitrate,
            "-ar", str(sample_rate),
            "-ac", str(channels)
        ]

    # ==================== CLEANUP ====================

    def _cleanup_watchdog(self):
        self.cleanup_tasks = [t for t in self.cleanup_tasks if not t.done()]

    def _emit_stage(
        self,
        stage_logger,
        event_type: str,
        status: str | None = None,
        error_text: str | None = None,
        extra: dict | None = None,
        stage: str | None = None,
    ):
        if not stage_logger:
            return

        payload = dict(extra or {})
        if stage:
            payload["stage"] = stage

        try:
            stage_logger(
                event_type=event_type,
                status=status,
                error_text=error_text,
                extra=payload,
            )
        except Exception:
            logger.exception("Не удалось записать этап video pipeline: %s", event_type)

    def _stage_started(self, stage_logger, stage: str, extra: dict | None = None):
        self._emit_stage(
            stage_logger,
            event_type="video_stage_started",
            status="started",
            stage=stage,
            extra=extra,
        )

    def _stage_completed(self, stage_logger, stage: str, extra: dict | None = None):
        self._emit_stage(
            stage_logger,
            event_type="video_stage_completed",
            status="completed",
            stage=stage,
            extra=extra,
        )

    def _stage_failed(
        self,
        stage_logger,
        stage: str,
        error_text: str | None = None,
        extra: dict | None = None,
    ):
        self._emit_stage(
            stage_logger,
            event_type="video_stage_failed",
            status="failed",
            error_text=error_text,
            stage=stage,
            extra=extra,
        )

    def check_disk_space(self):
        try:
            total, used, free = shutil.disk_usage(self.temp_dir)
            free_gb = free // (2 ** 30)
            logger.info(f"💾 Свободно на диске: {free_gb} GB")
            if free_gb < self.min_free_space_gb:
                logger.warning(f"⚠️ Мало места! Свободно {free_gb} GB")
                self.emergency_cleanup()
                return False
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка проверки диска: {e}")
            return False

    def emergency_cleanup(self):
        freed_space = 0
        now = time.time()
        logger.info("🧹 Запуск экстренной очистки...")
        for file in Path(self.temp_dir).glob('*'):
            if file.is_file():
                file_age = now - file.stat().st_mtime
                if file_age > 300:
                    size = file.stat().st_size
                    file.unlink()
                    freed_space += size
                    logger.info(f"   ✓ Удален старый файл: {file.name}")
        freed_gb = freed_space // (2 ** 30)
        logger.info(f"✅ Освобождено {freed_gb} GB")
        return freed_gb

    # ==================== PROBE ====================

    async def get_video_info(self, video_path, use_cache=True):
        if use_cache and video_path in self.video_info_cache:
            cached, timestamp = self.video_info_cache[video_path]
            if time.time() - timestamp < self.CACHE_TTL:
                return cached
            else:
                del self.video_info_cache[video_path]

        try:
            # === ПРОВЕРКА ФАЙЛА ===
            if not os.path.exists(video_path):
                logger.error(f"❌ Файл не найден: {video_path}")
                return None

            file_size = os.path.getsize(video_path)
            logger.debug(f"📁 Файл: {video_path} ({file_size/1024/1024:.2f} MB)")

            if file_size == 0:
                logger.error("❌ Файл пустой")
                return None

            # === ffprobe ===
            probe_cmd = [
                "ffprobe", "-v", "error",
                "-print_format", "json",
                "-show_format", "-show_streams",
                video_path
            ]

            process = await asyncio.create_subprocess_exec(
                *probe_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()
            stderr_text = stderr.decode("utf-8", errors="ignore").strip()

            if process.returncode != 0:
                logger.error(f"❌ ffprobe return code: {process.returncode}")
                logger.error(f"❌ stderr: {stderr_text if stderr_text else '[empty]'}")

                if "moov atom not found" in stderr_text:
                    logger.error("💥 Видео не завершено (битый MP4 / недокачан)")
                elif "Invalid data found" in stderr_text:
                    logger.error("💥 Файл не является корректным видео")

                return None

            info = json.loads(stdout)

            video_stream = None
            audio_stream = None

            for stream in info.get('streams', []):
                if stream['codec_type'] == 'video':
                    video_stream = stream
                elif stream['codec_type'] == 'audio':
                    audio_stream = stream

            if not video_stream:
                logger.error("❌ Видеопоток не найден")
                return None

            # FPS
            fps_parts = video_stream.get('r_frame_rate', '25/1').split('/')
            fps = 25
            try:
                if len(fps_parts) == 2:
                    numerator = float(fps_parts[0])
                    denominator = float(fps_parts[1])
                    if denominator != 0:
                        fps = numerator / denominator
            except:
                pass

            format_info = info.get('format', {})
            duration = float(format_info.get('duration', 0))
            
            # Получаем rotate_filter
            rotate_filter = self._get_rotate_filter(video_stream)
            
            # Получаем SAR
            sar = video_stream.get('sample_aspect_ratio', '1:1')
            
            # Получаем кодек
            codec_name = video_stream.get('codec_name')
            
            # Получаем информацию об аудио
            audio_sample_rate = None
            audio_channels = None
            if audio_stream:
                audio_sample_rate = audio_stream.get('sample_rate')
                audio_channels = audio_stream.get('channels')

            # 🔥 РАСШИРЕННЫЙ РЕЗУЛЬТАТ
            result = {
                'width': int(video_stream.get('width', 0)),
                'height': int(video_stream.get('height', 0)),
                'fps': round(fps, 3),
                'fps_normalized': self._normalize_fps(fps),
                'duration': duration,
                'has_audio': audio_stream is not None,
                'file_size': file_size,
                'pix_fmt': video_stream.get('pix_fmt', 'yuv420p'),
                'codec_name': codec_name,
                'sar': sar,
                'rotate_filter': rotate_filter,
                'audio_sample_rate': audio_sample_rate,
                'audio_channels': audio_channels,
            }

            self.video_info_cache[video_path] = (result, time.time())
            self._cleanup_cache()

            logger.info(f"🎬 Видео: {result['width']}x{result['height']} | {result['fps']:.2f} fps | {result['duration']:.1f} сек | кодек: {codec_name or 'unknown'} | SAR: {sar}")

            return result

        except Exception as e:
            logger.error(f"❌ Ошибка получения информации о видео: {e}", exc_info=True)
            return None

    # ==================== FFMPEG RUNNER ====================

    async def run_ffmpeg_with_progress(
        self,
        cmd,
        operation_name,
        timeout=600,
        total_duration_estimate=None,
        stage_logger=None,
    ):
        start_time = time.time()
        cmd = cmd.copy()
        cmd.extend(["-progress", "pipe:1", "-nostats"])

        logger.info(f"🎬 {operation_name}...")
        logger.info(f"   ⏱ Таймаут: {'НЕТ (watchdog)' if timeout is None else str(timeout) + ' сек'}")

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        last_out_time_ms = 0
        last_progress_update = time.time()
        current_fps = "N/A"
        current_speed = "N/A"
        current_size = "N/A"
        watchdog_triggered = False
        
        stderr_buffer = []
        MAX_STDERR_LINES = 500

        async def read_stdout():
            nonlocal last_out_time_ms, last_progress_update, current_fps, current_speed, current_size
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                line = line.decode("utf-8", errors="ignore").strip()
                if not line:
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    if key == "out_time_ms":
                        try:
                            new_time = int(value)
                            if new_time != last_out_time_ms:
                                last_progress_update = time.time()
                            last_out_time_ms = new_time
                        except:
                            pass
                    elif key == "fps":
                        current_fps = value
                    elif key == "speed":
                        current_speed = value
                    elif key == "total_size":
                        try:
                            current_size = f"{int(value) / (1024 * 1024):.1f} MB"
                        except:
                            pass
                    elif key == "progress":
                        now = time.time()
                        elapsed = now - start_time
                        processed_sec = last_out_time_ms / 1_000_000
                        if total_duration_estimate and total_duration_estimate > 0:
                            percent = min(100, (processed_sec / total_duration_estimate) * 100)
                            logger.info(
                                f"   📈 {operation_name}: {percent:.1f}% | "
                                f"{processed_sec:.1f}/{total_duration_estimate:.1f} сек | "
                                f"fps={current_fps} | speed={current_speed} | size={current_size} | elapsed={elapsed:.1f} сек"
                            )

                            self._emit_stage(
                                stage_logger,
                                event_type="video_ffmpeg_progress",
                                status="processing",
                                extra={
                                    "operation": operation_name,
                                    "percent": round(percent, 2),
                                    "processed_sec": round(processed_sec, 2),
                                    "total_sec": total_duration_estimate,
                                    "speed": current_speed,
                                },
                            )
                        else:
                            logger.info(
                                f"   📈 {operation_name}: "
                                f"processed={processed_sec:.1f} сек | "
                                f"fps={current_fps} | speed={current_speed} | size={current_size} | elapsed={elapsed:.1f} сек"
                            )

        async def read_stderr():
            while True:
                line = await process.stderr.readline()
                if not line:
                    break
                line = line.decode("utf-8", errors="ignore").strip()
                if line:
                    stderr_buffer.append(line)
                    if len(stderr_buffer) > MAX_STDERR_LINES:
                        stderr_buffer.pop(0)
                    if 'error' in line.lower() or 'warning' in line.lower():
                        logger.warning(f"   FFmpeg stderr: {line}")

        stdout_task = asyncio.create_task(read_stdout())
        stderr_task = asyncio.create_task(read_stderr())

        async def watchdog():
            nonlocal watchdog_triggered
            while not watchdog_triggered:
                await asyncio.sleep(30)
                if timeout is None and process.returncode is None:
                    if time.time() - last_progress_update > 300:
                        logger.error(f"   ⚠️ WATCHDOG: Нет прогресса >5 минут! Завершаю процесс.")
                        watchdog_triggered = True
                        process.terminate()
                        try:
                            await asyncio.wait_for(process.wait(), timeout=5)
                        except asyncio.TimeoutError:
                            process.kill()
                            await process.wait()

        watchdog_task = None
        if timeout is None:
            watchdog_task = asyncio.create_task(watchdog())

        try:
            if timeout is None:
                return_code = await process.wait()
            else:
                return_code = await asyncio.wait_for(process.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.error(f"   ❌ Таймаут {operation_name} (>{timeout} сек)")
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
            stdout_task.cancel()
            stderr_task.cancel()
            if watchdog_task:
                watchdog_task.cancel()
            return False

        if watchdog_task:
            watchdog_task.cancel()
        await stdout_task
        await stderr_task

        stderr = await process.stderr.read()
        stderr_text = stderr.decode("utf-8", errors="ignore").strip()
        if stderr_buffer:
            logger.debug(f"   📝 Сохранено {len(stderr_buffer)} строк stderr")
        elapsed_total = time.time() - start_time

        if return_code == 0:
            logger.info(f"   ✅ {operation_name} завершено за {elapsed_total:.1f} сек")
            return True
        else:
            logger.error(f"   ❌ {operation_name} завершилось с кодом {return_code}")
            if stderr_text:
                logger.error(f"   STDERR FFmpeg:\n{stderr_text[-4000:]}")
            return False

    # ==================== CUT ====================

    async def cut_video_fast(self, input_path, output_path, start_time, duration, stage_logger=None):
        cmd = [
            "ffmpeg",
            "-ss", str(start_time),
            "-i", input_path,
            "-t", str(duration),
            "-c", "copy",
            "-avoid_negative_ts", "make_zero",
            "-movflags", "+faststart",
            "-y", output_path
        ]
        result = await self.run_ffmpeg_with_progress(
            cmd,
            "Быстрая обрезка",
            timeout=300,
            total_duration_estimate=duration,
            stage_logger=stage_logger,
        )
        
        if result and os.path.exists(output_path):
            check_cmd = ["ffprobe", "-v", "error", "-select_streams", "v", 
                        "-show_entries", "stream=codec_type", "-of", "default=noprint_wrappers=1:nokey=1", output_path]
            process = await asyncio.create_subprocess_exec(*check_cmd, stdout=asyncio.subprocess.PIPE)
            stdout, _ = await process.communicate()
            if not stdout.strip():
                logger.error(f"   ❌ Файл не содержит видеопоток")
                os.remove(output_path)
                return False, start_time, 0
            
            info = await self.get_video_info(output_path, use_cache=False)
            if info:
                if info.get('sar') and info.get('sar') != '1:1':
                    logger.warning(f"   ⚠️ SAR не 1:1 ({info.get('sar')})")
                if abs(info['duration'] - duration) > 2:
                    logger.warning(f"   ⚠️ Длительность {info['duration']:.1f} вместо {duration:.1f}")
                    return False, start_time, 0
                return True, start_time, info['duration']
        
        if os.path.exists(output_path):
            os.remove(output_path)
        return False, start_time, 0

    async def normalize_video(self, input_path, output_path, profile, has_audio, duration, rotate_filter=None, stage_logger=None):
        logger.info(f"   🔄 Нормализация под {profile['target_width']}x{profile['target_height']} @ {profile['target_fps']}fps")
        
        input_info = await self.get_video_info(input_path, use_cache=False)
        
        vf = self._build_video_filter(
            profile['target_width'], profile['target_height'], profile['target_fps'],
            rotate_filter,
            original_width=input_info['width'],
            original_height=input_info['height'],
            original_fps=input_info['fps_normalized'],
            original_pix_fmt=input_info.get('pix_fmt')
        )
        
        cmd = ["ffmpeg", "-i", input_path]
        
        if vf != "null":
            cmd += ["-vf", vf]
        
        cmd += [
            *self._build_video_encode_args("veryfast", 26),
            *self._build_audio_encode_args(has_audio, input_info),
            "-movflags", "+faststart",
            "-y", output_path
        ]
        
        logger.info(f"   🎬 Команда: {' '.join(cmd)}")
        
        success = await self.run_ffmpeg_with_progress(
            cmd,
            "Нормализация",
            timeout=600,
            total_duration_estimate=duration,
            stage_logger=stage_logger,
        )
        if success and os.path.exists(output_path):
            info = await self.get_video_info(output_path, use_cache=False)
            if info:
                if info['width'] != profile['target_width'] or info['height'] != profile['target_height']:
                    logger.warning(f"   ⚠️ Размер не совпадает: {info['width']}x{info['height']} != {profile['target_width']}x{profile['target_height']}")
                    return False
                if info['fps_normalized'] != profile['target_fps']:
                    logger.warning(f"   ⚠️ FPS не совпадает: {info['fps_normalized']} != {profile['target_fps']}")
                    return False
                logger.info(f"   ✅ Нормализация успешна: {info['width']}x{info['height']} @ {info['fps_normalized']}fps")
                return True
        
        return False

    # ✅ ИСПРАВЛЕНО: безопасная сборка команды
    async def cut_video_accurate(self, input_path, output_path, start_time, duration, video_info, profile, stage_logger=None):
        logger.info("   🔄 Точная обрезка с перекодированием")
        
        vf = self._build_video_filter(
            profile['target_width'], profile['target_height'], profile['target_fps'],
            video_info.get('rotate_filter'),
            original_width=video_info['width'],
            original_height=video_info['height'],
            original_fps=video_info['fps_normalized'],
            original_pix_fmt=video_info.get('pix_fmt')
        )
        
        # Собираем команду безопасно
        cmd = ["ffmpeg", "-ss", str(start_time), "-i", input_path, "-t", str(duration)]
        
        if vf != "null":
            cmd += ["-vf", vf]
        
        cmd += [
            "-fps_mode", "cfr",
            *self._build_video_encode_args("veryfast", 26),
            *self._build_audio_encode_args(video_info['has_audio'], video_info),
            "-movflags", "+faststart",
            "-y", output_path
        ]
        
        return await self.run_ffmpeg_with_progress(
            cmd,
            "Точная обрезка",
            timeout=600,
            total_duration_estimate=duration,
            stage_logger=stage_logger,
        )

    # ==================== INTRO ====================

    async def create_intro_matching_video(self, source_path, profile, duration, stage_logger=None):
        output_path = os.path.join(self.temp_dir, f"intro_matched_{int(time.time())}.mp4")
        is_image = source_path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'))
        
        vf = self._build_video_filter(profile['target_width'], profile['target_height'], profile['target_fps'])
        
        if is_image:
            logger.info(f"   🖼️ Заставка из изображения")
            cmd = [
                "ffmpeg", "-loop", "1", "-i", source_path,
                "-vf", vf,
                *self._build_video_encode_args("veryfast", 26),
                "-an", "-t", str(duration),
                "-movflags", "+faststart",
                "-y", output_path
            ]
        else:
            logger.info(f"   🎬 Заставка из видео")
            cmd = [
                "ffmpeg", "-i", source_path,
                "-vf", vf,
                *self._build_video_encode_args("veryfast", 26),
                "-an", "-t", str(duration),
                "-movflags", "+faststart",
                "-y", output_path
            ]

        result = await self.run_ffmpeg_with_progress(
            cmd,
            "Создание заставки",
            timeout=60,
            total_duration_estimate=duration,
            stage_logger=stage_logger,
        )
        if result and os.path.exists(output_path):
            logger.info(f"   ✅ Заставка создана: {os.path.getsize(output_path) / (1024 * 1024):.1f} MB")
            return output_path
        return None

    async def create_thumbnail_fast(self, video_path, thumbnail_path, stage_logger=None):
        if not video_path or not os.path.isfile(video_path):
            return False
        cmd = [
            "ffmpeg",
            "-i", video_path,
            "-vf", "scale=640:-1",
            "-vframes", "1",
            "-q:v", "3",
            "-y", thumbnail_path
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await process.wait()

        if process.returncode == 0 and os.path.isfile(thumbnail_path):
            logger.info(f"   🖼️ Превью создано: {os.path.getsize(thumbnail_path) / 1024:.0f} KB")

            return True

        return False

    # ==================== CONCAT ====================

    async def concat_videos_safe(self, intro_path, main_path, output_path, profile, has_audio, expected_duration=None, stage_logger=None):
        w = profile['target_width']
        h = profile['target_height']
        fps = profile['target_fps']

        logger.info("   🔗 Склейка с нормализацией параметров")
        logger.info(f"      Цель: {w}x{h} @ {fps} fps")

        intro_info = await self.get_video_info(intro_path, use_cache=False)
        main_info = await self.get_video_info(main_path, use_cache=False)
        intro_duration = intro_info['duration'] if intro_info else 5

        # ✅ ИСПРАВЛЕНО: добавлена проверка pix_fmt
        intro_needs_normalize = (
            intro_info and (
                intro_info['width'] != w or
                intro_info['height'] != h or
                intro_info['fps_normalized'] != fps or
                intro_info.get('sar', '1:1') != '1:1' or
                intro_info.get('pix_fmt', 'yuv420p') != 'yuv420p'
            )
        )
        
        # ✅ ИСПРАВЛЕНО: добавлена проверка pix_fmt
        main_needs_normalize = (
            main_info and (
                main_info['width'] != w or
                main_info['height'] != h or
                main_info['fps_normalized'] != fps or
                main_info.get('sar', '1:1') != '1:1' or
                main_info.get('pix_fmt', 'yuv420p') != 'yuv420p'
            )
        )

        if intro_needs_normalize:
            vf_intro = self._build_video_filter(w, h, fps,
                                                original_width=intro_info['width'],
                                                original_height=intro_info['height'],
                                                original_fps=intro_info['fps_normalized'],
                                                original_pix_fmt=intro_info.get('pix_fmt'))
        else:
            vf_intro = "null"
        
        if main_needs_normalize:
            vf_main = self._build_video_filter(w, h, fps,
                                               original_width=main_info['width'],
                                               original_height=main_info['height'],
                                               original_fps=main_info['fps_normalized'],
                                               original_pix_fmt=main_info.get('pix_fmt'))
        else:
            vf_main = "null"

        if vf_intro == "null" and vf_main == "null":
            filter_complex = "[0:v]null[v0];[1:v]null[v1];[v0][v1]concat=n=2:v=1:a=0[outv]"
        elif vf_intro == "null":
            filter_complex = f"[0:v]null[v0];[1:v]{vf_main}[v1];[v0][v1]concat=n=2:v=1:a=0[outv]"
        elif vf_main == "null":
            filter_complex = f"[0:v]{vf_intro}[v0];[1:v]null[v1];[v0][v1]concat=n=2:v=1:a=0[outv]"
        else:
            filter_complex = f"[0:v]{vf_intro}[v0];[1:v]{vf_main}[v1];[v0][v1]concat=n=2:v=1:a=0[outv]"
        
        if has_audio:
            filter_complex += f";[1:a]aresample={self.target_sample_rate},aformat=sample_fmts=fltp:channel_layouts=stereo,adelay={intro_duration * 1000}|{intro_duration * 1000}[outa]"
            
            cmd = [
                "ffmpeg", "-y", "-i", intro_path, "-i", main_path,
                "-filter_complex", filter_complex,
                "-map", "[outv]", "-map", "[outa]",
                *self._build_video_encode_args("veryfast", 26),
                *self._build_audio_encode_args(True, main_info),
                "-movflags", "+faststart", output_path
            ]
        else:
            cmd = [
                "ffmpeg", "-y", "-i", intro_path, "-i", main_path,
                "-filter_complex", filter_complex,
                "-map", "[outv]",
                *self._build_video_encode_args("veryfast", 26),
                "-an",
                "-movflags", "+faststart", output_path
            ]

        result = await self.run_ffmpeg_with_progress(
            cmd,
            "Склейка",
            timeout=None,
            total_duration_estimate=expected_duration,
            stage_logger=stage_logger,
        )

        if result and os.path.exists(output_path):
            logger.info(f"   ✅ Склейка завершена, размер: {os.path.getsize(output_path) / (1024 * 1024):.1f} MB")
            result_info = await self.get_video_info(output_path, use_cache=False)
            if result_info:
                logger.info(f"   🔍 Проверка: {result_info['width']}x{result_info['height']} @ {result_info['fps']:.2f} fps, аудио: {'✅' if result_info['has_audio'] else '❌'}")
            return True
        return False

    # ==================== SEND ====================

    def restore_caption_entities(self, caption_entities_json):
        """Восстанавливает caption_entities из JSON/списка словарей для send_video"""
        if not caption_entities_json:
            return None

        try:
            entities_data = caption_entities_json

            if isinstance(entities_data, str):
                raw_text = entities_data.strip()
                if not raw_text:
                    return None

                try:
                    entities_data = json.loads(raw_text)
                except Exception:
                    import ast
                    try:
                        entities_data = ast.literal_eval(raw_text)
                    except Exception as exc:
                        logger.error(
                            "❌ Ошибка восстановления caption_entities: не удалось распарсить строку | error=%s | preview=%r",
                            exc,
                            raw_text[:300],
                        )
                        return None

            if isinstance(entities_data, str):
                try:
                    entities_data = json.loads(entities_data)
                except Exception:
                    logger.error(
                        "❌ Ошибка восстановления caption_entities: после parse данные остались строкой"
                    )
                    return None

            if isinstance(entities_data, dict):
                entities_data = [entities_data]

            if not isinstance(entities_data, list) or not entities_data:
                return None

            caption_entities = []

            for e in entities_data:
                if not isinstance(e, dict):
                    continue

                raw_type = str(e.get("type") or "").strip().lower()
                if not raw_type:
                    continue

                try:
                    offset = int(e["offset"])
                    length = int(e["length"])
                except Exception:
                    continue

                if offset < 0 or length <= 0:
                    continue

                if "custom_emoji" in raw_type:
                    entity_type = "custom_emoji"
                elif "text_link" in raw_type:
                    entity_type = "text_link"
                elif "bold" in raw_type:
                    entity_type = "bold"
                elif "italic" in raw_type:
                    entity_type = "italic"
                elif "underline" in raw_type:
                    entity_type = "underline"
                elif "strikethrough" in raw_type or raw_type == "strike":
                    entity_type = "strikethrough"
                elif "spoiler" in raw_type:
                    entity_type = "spoiler"
                elif raw_type == "code":
                    entity_type = "code"
                elif raw_type == "pre":
                    entity_type = "pre"
                elif raw_type == "blockquote":
                    entity_type = "blockquote"
                elif raw_type == "url":
                    entity_type = "url"
                elif raw_type == "mention":
                    entity_type = "mention"
                elif raw_type == "email":
                    entity_type = "email"
                elif raw_type == "phone":
                    entity_type = "phone"
                elif raw_type == "hashtag":
                    entity_type = "hashtag"
                elif raw_type == "cashtag":
                    entity_type = "cashtag"
                elif raw_type == "bot_command":
                    entity_type = "bot_command"
                else:
                    entity_type = raw_type

                entity = MessageEntity(
                    type=entity_type,
                    offset=offset,
                    length=length,
                    url=e.get("url"),
                    language=e.get("language"),
                    custom_emoji_id=e.get("custom_emoji_id") if entity_type == "custom_emoji" else None,
                )
                caption_entities.append(entity)

            logger.info(
                "VIDEO_CAPTION_MODE | restore_caption_entities | in=%s | out=%s",
                len(entities_data),
                len(caption_entities),
            )

            return caption_entities if caption_entities else None

        except Exception as e:
            logger.error(f"❌ Ошибка восстановления caption_entities: {e}")
            return None

    async def send_with_retry(
        self,
        bot,
        destination_channel,
        target_thread_id,
        video_path,
        thumbnail_path,
        caption,
        duration,
        caption_entities_json=None,
        caption_send_mode="plain",
        max_retries=3,
        stage_logger=None,
    ):
        self._stage_started(
            stage_logger,
            "send",
            {
                "destination_channel": str(destination_channel),
                "target_thread_id": target_thread_id,
                "video_path": video_path,
                "thumbnail_path": thumbnail_path,
                "duration": float(duration),
                "max_retries": max_retries,
                "caption_send_mode": caption_send_mode,
            },
        )

        normalized_mode = str(caption_send_mode or "plain").strip().lower()
        if normalized_mode not in ("plain", "premium"):
            logger.warning("Неизвестный caption_send_mode=%s, fallback -> plain", normalized_mode)
            normalized_mode = "plain"

        for attempt in range(max_retries):
            try:
                if normalized_mode == "premium":
                    logger.info("📤 Отправка видео через Telethon (premium mode)")

                    sent_msg = await self._send_video_via_telethon(
                        destination_channel=destination_channel,
                        target_thread_id=target_thread_id,
                        video_path=video_path,
                        thumbnail_path=thumbnail_path,
                        caption=caption,
                        caption_entities_json=caption_entities_json,
                    )
                else:
                    logger.info("📤 Отправка видео через Bot API (plain mode)")

                    video_input = FSInputFile(video_path, filename=f"video_{int(time.time())}.mp4")

                    send_kwargs = {
                        "chat_id": destination_channel,
                        "message_thread_id": target_thread_id,
                        "video": video_input,
                        "caption": caption,
                        "supports_streaming": True,
                        "duration": int(duration),
                    }

                    if thumbnail_path and os.path.isfile(thumbnail_path):
                        send_kwargs["thumbnail"] = FSInputFile(thumbnail_path, filename="thumb.jpg")

                    sent_msg = await bot.send_video(**send_kwargs)

                logger.info("   ✅ Видео отправлено (попытка %s)", attempt + 1)

                sent_message_id = getattr(sent_msg, "message_id", None)
                if sent_message_id is None:
                    sent_message_id = getattr(sent_msg, "id", None)

                self._stage_completed(
                    stage_logger,
                    "send",
                    {
                        "attempt": attempt + 1,
                        "message_id": sent_message_id,
                        "caption_send_mode": normalized_mode,
                        "transport": "telethon" if normalized_mode == "premium" else "bot_api",
                        "file_id": getattr(getattr(sent_msg, "video", None), "file_id", None),
                    },
                )

                return sent_msg

            except Exception as e:
                logger.error("   ❌ Ошибка отправки (%s/%s): %s", attempt + 1, max_retries, e)

                self._emit_stage(
                    stage_logger,
                    event_type="video_send_retry",
                    status="processing" if attempt < max_retries - 1 else "faulty",
                    error_text=str(e),
                    extra={
                        "attempt": attempt + 1,
                        "max_retries": max_retries,
                        "caption_send_mode": normalized_mode,
                        "transport": "telethon" if normalized_mode == "premium" else "bot_api",
                    },
                )

                if attempt < max_retries - 1:
                    await asyncio.sleep(5 * (attempt + 1))

        self._stage_failed(
            stage_logger,
            "send",
            error_text="Не удалось отправить видео после всех попыток",
            extra={
                "destination_channel": str(destination_channel),
                "target_thread_id": target_thread_id,
                "max_retries": max_retries,
                "caption_send_mode": normalized_mode,
            },
        )

        return None

    async def delayed_cleanup(self, file_paths, delay=300):
        async def cleanup():
            await asyncio.sleep(delay)
            logger.info(f"🧹 Отложенная очистка...")
            deleted = 0
            for fp in file_paths:
                try:
                    if fp and os.path.isfile(fp):
                        size = os.path.getsize(fp) / (1024 ** 2)
                        os.remove(fp)
                        deleted += 1
                        logger.info(f"   ✓ Удален: {os.path.basename(fp)} ({size:.1f} MB)")
                except Exception as e:
                    logger.error(f"   ✗ Ошибка удаления {fp}: {e}")
            logger.info(f"   ✅ Удалено {deleted} файлов")

        task = asyncio.create_task(cleanup())

        def remove_task(t):
            try:
                self.cleanup_tasks.remove(t)
            except ValueError:
                pass

        task.add_done_callback(remove_task)
        self.cleanup_tasks.append(task)
        self._cleanup_watchdog()

    # ==================== MAIN PROCESS ====================

    async def process_video(
        self,
        video_file_id,
        context,
        destination_channel,
        target_thread_id=None,
        add_intro=False,
        intro_name_horizontal=None,
        intro_name_vertical=None,
        caption="",
        caption_entities_json=None,
        caption_send_mode="plain",
        input_file_path=None,
        stage_logger=None,
    ):
        async with self.semaphore:
            return await self._process_video_internal(
                video_file_id,
                context,
                destination_channel,
                target_thread_id,
                add_intro,
                intro_name_horizontal,
                intro_name_vertical,
                caption,
                caption_entities_json,
                caption_send_mode,
                input_file_path,
                stage_logger,
                send_output=True,
            )

    async def build_processed_video(
        self,
        *,
        input_file_path,
        add_intro=False,
        intro_name_horizontal=None,
        intro_name_vertical=None,
        stage_logger=None,
    ):
        async with self.semaphore:
            return await self._process_video_internal(
                video_file_id=None,
                context=None,
                destination_channel=None,
                target_thread_id=None,
                add_intro=add_intro,
                intro_name_horizontal=intro_name_horizontal,
                intro_name_vertical=intro_name_vertical,
                caption="",
                caption_entities_json=None,
                caption_send_mode="plain",
                input_file_path=input_file_path,
                stage_logger=stage_logger,
                send_output=False,
            )

    async def _process_video_internal(
        self,
        video_file_id,
        context,
        destination_channel,
        target_thread_id=None,
        add_intro=False,
        intro_name_horizontal=None,
        intro_name_vertical=None,
        caption="",
        caption_entities_json=None,
        caption_send_mode="plain",
        input_file_path=None,
        stage_logger=None,
        send_output=True,
    ):
        logger.info("=" * 70)
        logger.info("🎬 НАЧАЛО ОБРАБОТКИ ВИДЕО")
        logger.info("=" * 70)

        self._stage_started(
            stage_logger,
            "pipeline",
            {
                "video_file_id": video_file_id,
                "destination_channel": str(destination_channel),
                "target_thread_id": target_thread_id,
                "add_intro": bool(add_intro),
                "intro_name_horizontal": intro_name_horizontal,
                "intro_name_vertical": intro_name_vertical,
                "input_file_path": str(input_file_path) if input_file_path else None,
                "caption_send_mode": caption_send_mode,
            },
        )

        if not video_file_id and not input_file_path:
            logger.error("❌ Не передан ни file_id, ни путь к файлу")
            self._stage_failed(
                stage_logger,
                "pipeline",
                error_text="Не передан ни file_id, ни путь к файлу",
            )
            return False

        if not self.check_disk_space():
            self._stage_failed(
                stage_logger,
                "pipeline",
                error_text="Недостаточно свободного места на диске",
            )
            return False

        timestamp = int(time.time())
        base_name = f"video_{timestamp}_{video_file_id[:8] if video_file_id else 'local'}"

        input_path = input_file_path or os.path.join(self.temp_dir, f"input_{base_name}.mp4")
        clipped_main_path = os.path.join(self.temp_dir, f"clipped_{base_name}.mp4")
        intro_processed_path = os.path.join(self.temp_dir, f"intro_processed_{base_name}.mp4")
        final_output = os.path.join(self.temp_dir, f"final_{base_name}.mp4")
        normalized_main_path = os.path.join(self.temp_dir, f"normalized_{base_name}.mp4")
        thumbnail_path = os.path.join(self.temp_dir, f"thumb_{base_name}.jpg")

        files_to_delete = [input_path, clipped_main_path, final_output, normalized_main_path, thumbnail_path]

        try:
            bot = context.bot if hasattr(context, 'bot') else self.bot

            # DOWNLOAD
            if not input_file_path:
                logger.info("📥 [1/8] Скачивание видео...")
                self._stage_started(
                    stage_logger,
                    "download",
                    {
                        "video_file_id": video_file_id,
                    },
                )
                try:
                    file = await bot.get_file(video_file_id)
                    await file.download_to_drive(input_path)
                except Exception as e:
                    self._stage_failed(
                        stage_logger,
                        "download",
                        error_text=str(e),
                        extra={
                            "video_file_id": video_file_id,
                        },
                    )
                    raise

                self._stage_completed(
                    stage_logger,
                    "download",
                    {
                        "input_path": input_path,
                        "file_size_mb": round(os.path.getsize(input_path) / (1024 ** 2), 2),
                    },
                )
            else:
                logger.info(f"📁 [1/8] Использую локальный файл: {input_path}")

            if not os.path.isfile(input_path):
                logger.error("❌ Входной файл не найден")

                self._stage_failed(
                    stage_logger,
                    "pipeline",
                    error_text="Входной файл не найден",
                )

                return False

            # ANALYZE
            logger.info("🔍 [2/8] Анализ видео...")
            self._stage_started(
                stage_logger,
                "probe",
                {"input_path": input_path},
            )

            video_info = await self.get_video_info(input_path)
            if not video_info:
                logger.error("❌ Не удалось получить информацию о видео")
                self._stage_failed(
                    stage_logger,
                    "probe",
                    error_text="Не удалось получить информацию о видео",
                    extra={"input_path": input_path},
                )
                return False

            self._stage_completed(
                stage_logger,
                "probe",
                {
                    "input_path": input_path,
                    "width": video_info["width"],
                    "height": video_info["height"],
                    "fps": video_info["fps"],
                    "fps_normalized": video_info["fps_normalized"],
                    "duration": video_info["duration"],
                    "has_audio": video_info["has_audio"],
                    "codec_name": video_info.get("codec_name"),
                    "pix_fmt": video_info.get("pix_fmt"),
                    "sar": video_info.get("sar"),
                },
            )

            max_duration = config.max_input_duration
            if video_info['duration'] > max_duration:
                logger.error(f"❌ Видео слишком длинное: {video_info['duration']:.0f} сек")

                self._stage_failed(
                    stage_logger,
                    "pipeline",
                    error_text="Видео превышает допустимую длительность",
                    extra={
                        "duration": video_info["duration"],
                        "max_duration": max_duration,
                    },
                )

                return False

            logger.info(f"   📐 Разрешение: {video_info['width']}x{video_info['height']}")
            logger.info(f"   🎞️ FPS: {video_info['fps']:.2f} (норм: {video_info['fps_normalized']})")
            logger.info(f"   🎥 Кодек: {video_info.get('codec_name', 'unknown')}")
            logger.info(f"   ⏱ Длительность: {video_info['duration']:.1f} сек")
            logger.info(f"   🔊 Аудио: {'✅' if video_info['has_audio'] else '❌'}")
            logger.info(f"   💾 Свободно: {self._get_free_space_gb()} GB")

            # PROFILE
            profile = self._get_processing_profile(video_info)

            self._emit_stage(
                stage_logger,
                event_type="video_profile_selected",
                status="processing",
                extra={
                    "target_width": profile["target_width"],
                    "target_height": profile["target_height"],
                    "target_fps": profile["target_fps"],
                    "downscale": profile["downscale"],
                    "is_vertical": profile["is_vertical"],
                    "reason": profile["reason"],
                },
            )

            logger.info("🧠 [2.1/8] Выбран профиль обработки:")
            logger.info(f"   Исходник: {video_info['width']}x{video_info['height']} @ {video_info['fps']:.2f} fps")
            logger.info(f"   Цель:     {profile['target_width']}x{profile['target_height']} @ {profile['target_fps']} fps")
            logger.info(f"   Downscale: {'ДА' if profile['downscale'] else 'НЕТ'}")
            logger.info(f"   Причина:  {profile['reason']}")

            intro_duration = config.intro_duration
            target_duration = config.max_video_duration
            main_cut_duration = target_duration - intro_duration

            if main_cut_duration < 0:
                logger.warning("⚠️ Заставка длиннее целевого видео")
                main_cut_duration = target_duration
                add_intro = False

            # CUT
            if video_info['duration'] > main_cut_duration:
                start_time = (video_info['duration'] - main_cut_duration) / 2
                logger.info(f"✂️ [3/8] Обрезка ({start_time:.1f}с -> {main_cut_duration}с)")
                self._stage_started(
                    stage_logger,
                    "trim",
                    {
                        "start_time": float(start_time),
                        "duration": float(main_cut_duration),
                        "mode": "fast_then_accurate_fallback",
                    },
                )
                success, exact_start, exact_duration = await self.cut_video_fast(input_path, clipped_main_path, start_time, main_cut_duration, stage_logger=stage_logger)

                if success:
                    clipped_info = await self.get_video_info(clipped_main_path, use_cache=False)

                    self._stage_completed(
                        stage_logger,
                        "trim",
                        {
                            "final_duration": float(exact_duration),
                        },
                    )
                    needs_normalize = (
                        clipped_info['width'] != profile['target_width'] or
                        clipped_info['height'] != profile['target_height'] or
                        clipped_info['fps_normalized'] != profile['target_fps'] or
                        clipped_info.get('sar', '1:1') != '1:1' or
                        clipped_info.get('pix_fmt', 'yuv420p') != 'yuv420p'
                    )
                    if needs_normalize or profile['downscale']:
                        logger.info(f"   🔄 Требуется нормализация")

                        self._stage_started(
                            stage_logger,
                            "normalize",
                            {
                                "target_width": profile["target_width"],
                                "target_height": profile["target_height"],
                                "target_fps": profile["target_fps"],
                            },
                        )
                        if await self.normalize_video(
                            clipped_main_path,
                            normalized_main_path,
                            profile,
                            video_info['has_audio'],
                            exact_duration,
                            video_info.get('rotate_filter'),
                            stage_logger=stage_logger,
                        ):
                            os.replace(normalized_main_path, clipped_main_path)

                            self._stage_completed(
                                stage_logger,
                                "normalize",
                                {
                                    "target_width": profile["target_width"],
                                    "target_height": profile["target_height"],
                                    "target_fps": profile["target_fps"],
                                },
                            )
                        else:
                            logger.error(f"❌ Нормализация не удалась, обработка прервана")

                            self._stage_failed(
                                stage_logger,
                                "normalize",
                                error_text="Нормализация не удалась",
                                extra={
                                    "target_width": profile["target_width"],
                                    "target_height": profile["target_height"],
                                    "target_fps": profile["target_fps"],
                                },
                            )

                            return False
                else:
                    logger.warning("   ⚠️ Быстрая обрезка не удалась, пробую точную")
                    if not await self.cut_video_accurate(input_path, clipped_main_path, start_time, main_cut_duration, video_info, profile, stage_logger=stage_logger):
                        logger.error("❌ Не удалось обрезать видео")

                        self._stage_failed(
                            stage_logger,
                            "trim",
                            error_text="Не удалось обрезать видео",
                        )

                        return False
            else:
                logger.info(f"📹 [3/8] Видео короче {main_cut_duration}с, копирую")
                shutil.copy2(input_path, clipped_main_path)

                clipped_info = await self.get_video_info(clipped_main_path, use_cache=False)
                needs_normalize = (
                    clipped_info['width'] != profile['target_width'] or
                    clipped_info['height'] != profile['target_height'] or
                    clipped_info['fps_normalized'] != profile['target_fps'] or
                    clipped_info.get('sar', '1:1') != '1:1' or
                    clipped_info.get('pix_fmt', 'yuv420p') != 'yuv420p'
                )
                if needs_normalize or profile['downscale']:
                    logger.info(f"   🔄 Требуется нормализация")
                    self._stage_started(
                        stage_logger,
                        "normalize",
                        {
                            "target_width": profile["target_width"],
                            "target_height": profile["target_height"],
                            "target_fps": profile["target_fps"],
                        },
                    )

                    if await self.normalize_video(
                        clipped_main_path,
                        normalized_main_path,
                        profile,
                        video_info['has_audio'],
                        video_info['duration'],
                        video_info.get('rotate_filter'),
                        stage_logger=stage_logger,
                    ):
                        os.replace(normalized_main_path, clipped_main_path)

                        self._stage_completed(
                            stage_logger,
                            "normalize",
                            {
                                "target_width": profile["target_width"],
                                "target_height": profile["target_height"],
                                "target_fps": profile["target_fps"],
                            },
                        )
                    else:
                        logger.error(f"❌ Нормализация не удалась, обработка прервана")

                        self._stage_failed(
                            stage_logger,
                            "normalize",
                            error_text="Нормализация не удалась",
                            extra={
                                "target_width": profile["target_width"],
                                "target_height": profile["target_height"],
                                "target_fps": profile["target_fps"],
                            },
                        )

                        return False

            clipped_info = await self.get_video_info(clipped_main_path)
            logger.info(f"   📦 Размер: {clipped_info['file_size'] / (1024 * 1024):.1f} MB")

            # INTRO
            intro_created = False
            intro_info = None

            selected_intro_name = None

            if profile.get("is_vertical"):
                selected_intro_name = intro_name_vertical
                logger.info("📱 Обнаружено вертикальное видео -> выбираю вертикальную заставку")
            else:
                selected_intro_name = intro_name_horizontal
                logger.info("🎬 Обнаружено горизонтальное видео -> выбираю горизонтальную заставку")

            if add_intro and selected_intro_name:
                intro_source_path = os.path.join(self.intros_dir, selected_intro_name)

                if os.path.isfile(intro_source_path):
                    logger.info("🎬 [4/8] Создание заставки под целевой профиль...")

                    self._stage_started(
                        stage_logger,
                        "intro",
                        {
                            "intro_name": selected_intro_name,
                            "intro_source_path": intro_source_path,
                            "target_width": profile["target_width"],
                            "target_height": profile["target_height"],
                            "target_fps": profile["target_fps"],
                        },
                    )
                    logger.info(f"   Выбранная заставка: {selected_intro_name}")
                    logger.info(f"   Цель: {profile['target_width']}x{profile['target_height']} @ {profile['target_fps']}fps")

                    intro_result = await self.create_intro_matching_video(
                        intro_source_path,
                        profile,
                        intro_duration,
                        stage_logger=stage_logger
                    )

                    if intro_result and os.path.exists(intro_result):
                        intro_processed_path = intro_result
                        intro_created = True
                        intro_info = await self.get_video_info(intro_processed_path)
                        files_to_delete.append(intro_processed_path)
                        logger.info(f"   📦 Размер заставки: {intro_info['file_size'] / (1024 * 1024):.1f} MB")

                        self._stage_completed(
                            stage_logger,
                            "intro",
                            {
                                "intro_name": selected_intro_name,
                                "output_path": intro_processed_path,
                            },
                        )
                    else:
                        self._stage_failed(
                            stage_logger,
                            "intro",
                            error_text="Не удалось подготовить заставку",
                            extra={
                                "intro_name": selected_intro_name,
                                "intro_source_path": intro_source_path,
                            },
                        )
                else:
                    logger.warning(f"⚠️ Файл заставки не найден: {intro_source_path}")
            elif add_intro:
                logger.info("ℹ️ Заставка включена, но подходящий файл для текущей ориентации не выбран")

            # CONCAT
            if intro_created and os.path.isfile(intro_processed_path) and intro_info:
                logger.info("🔗 [5/8] Склейка видео...")

                self._stage_started(
                    stage_logger,
                    "concat",
                    {
                        "with_intro": True,
                    },
                )
                if not await self.concat_videos_safe(intro_processed_path, clipped_main_path, final_output, profile,
                                                    video_info['has_audio'], expected_duration=intro_duration + clipped_info['duration'], stage_logger=stage_logger):
                    logger.error("❌ Ошибка при склейке видео")

                    self._stage_failed(
                        stage_logger,
                        "concat",
                        error_text="Ошибка при склейке видео",
                    )

                    return False

                self._stage_completed(
                    stage_logger,
                    "concat",
                    {
                        "with_intro": True,
                        "output_path": final_output,
                    },
                )
            else:
                logger.info("🔗 [5/8] Склейка не требуется")
                shutil.copy2(clipped_main_path, final_output)

                self._stage_completed(
                    stage_logger,
                    "concat",
                    {
                        "with_intro": False,
                        "mode": "copy_without_concat",
                        "output_path": final_output,
                    },
                )

            # THUMBNAIL
            logger.info("🖼️ [6/8] Создание превью...")
            self._stage_started(
                stage_logger,
                "thumbnail",
                {
                    "video_path": final_output,
                    "thumbnail_path": thumbnail_path,
                },
            )

            thumb_success = await self.create_thumbnail_fast(final_output, thumbnail_path)

            if thumb_success:
                self._stage_completed(
                    stage_logger,
                    "thumbnail",
                    {
                        "thumbnail_path": thumbnail_path,
                    },
                )
            else:
                self._stage_failed(
                    stage_logger,
                    "thumbnail",
                    error_text="Не удалось создать превью",
                    extra={
                        "thumbnail_path": thumbnail_path,
                    },
                )

            logger.info("✅ [7/8] Финальная проверка...")
            final_info = await self.get_video_info(final_output)

            if not final_info:
                logger.error("❌ Не удалось проанализировать финальный файл")
                return False

            if final_info['duration'] <= 0:
                logger.error(f"❌ Финальный файл имеет нулевую длительность: {final_info['duration']}")
                return False

            if final_info['width'] <= 0 or final_info['height'] <= 0:
                logger.error(f"❌ Финальный файл имеет некорректное разрешение: {final_info['width']}x{final_info['height']}")
                return False

            if profile['downscale']:
                if final_info['width'] > profile['target_width'] or final_info['height'] > profile['target_height']:
                    logger.error(f"❌ Финальный файл не соответствует downscale: {final_info['width']}x{final_info['height']} > {profile['target_width']}x{profile['target_height']}")
                    return False

            if video_info['has_audio'] != final_info['has_audio']:
                logger.warning(f"⚠️ Аудио не соответствует: исходное {video_info['has_audio']}, финал {final_info['has_audio']}")

            logger.info(f"   📐 Разрешение: {final_info['width']}x{final_info['height']}")
            logger.info(f"   🎞️ FPS: {final_info['fps']:.2f}")
            logger.info(f"   ⏱ Длительность: {final_info['duration']:.1f} сек")
            logger.info(f"   📊 Размер: {final_info['file_size'] / (1024 * 1024):.1f} MB")
            logger.info(f"   💾 Свободно: {self._get_free_space_gb()} GB")
            logger.info(f"   ✅ Проверка пройдена")

            if send_output:
                # SEND
                logger.info(f"📤 [8/8] Отправка видео в канал {destination_channel}...")
                sent_msg = await self.send_with_retry(
                    bot,
                    destination_channel,
                    target_thread_id,
                    final_output,
                    thumbnail_path if thumb_success else None,
                    caption,
                    final_info['duration'],
                    caption_entities_json=caption_entities_json,
                    caption_send_mode=caption_send_mode,
                    stage_logger=stage_logger,
                )
                if not sent_msg:
                    logger.error("❌ Не удалось отправить видео")
                    return sent_msg

            for path in [input_path, clipped_main_path, intro_processed_path, final_output, normalized_main_path]:
                if path in self.video_info_cache:
                    del self.video_info_cache[path]

            if send_output:
                logger.info("⏰ Планирование отложенной очистки...")
                await self.delayed_cleanup(files_to_delete, delay=300)

            logger.info("=" * 70)
            logger.info("✅ ОБРАБОТКА ВИДЕО ЗАВЕРШЕНА")
            logger.info("=" * 70)

            self._stage_completed(
                stage_logger,
                "pipeline",
                {
                    "final_output": final_output,
                    "final_duration": final_info["duration"],
                    "final_width": final_info["width"],
                    "final_height": final_info["height"],
                    "final_fps": final_info["fps"],
                    "final_size_mb": round(final_info["file_size"] / (1024 * 1024), 2),
                    "caption_send_mode": caption_send_mode,
                },
            )

            if send_output:
                return sent_msg

            artifact_payload = {
                "processed_video_path": final_output,
                "thumbnail_path": thumbnail_path if thumb_success and os.path.isfile(thumbnail_path) else None,
                "duration": final_info["duration"],
                "width": final_info["width"],
                "height": final_info["height"],
                "has_intro": bool(intro_created),
                "trim_applied": bool(video_info["duration"] > main_cut_duration),
                "processing_summary": {
                    "target_width": profile["target_width"],
                    "target_height": profile["target_height"],
                    "target_fps": profile["target_fps"],
                    "downscale": profile["downscale"],
                },
                "cleanup_paths": [p for p in files_to_delete if p and p != final_output],
            }
            return artifact_payload

        except Exception as e:
            logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            logger.error(traceback.format_exc())

            self._stage_failed(
                stage_logger,
                "pipeline",
                error_text=str(e),
                extra={
                    "input_file_path": input_file_path,
                    "destination_channel": str(destination_channel),
                    "caption_send_mode": caption_send_mode,
                },
            )

            for fp in files_to_delete:
                try:
                    if fp and os.path.isfile(fp):
                        os.remove(fp)
                    if fp in self.video_info_cache:
                        del self.video_info_cache[fp]
                except:
                    pass
            return False

    async def shutdown(self):
        logger.info("🛑 Завершение работы VideoProcessor...")
        self._cleanup_watchdog()
        if self.cleanup_tasks:
            logger.info(f"⏳ Ожидание {len(self.cleanup_tasks)} задач...")
            await asyncio.gather(*self.cleanup_tasks, return_exceptions=True)
