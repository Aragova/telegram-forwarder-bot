from PIL import Image, ImageDraw, ImageFont
from pathlib import Path

OUT = Path("landing/assets/preview.png")
OUT.parent.mkdir(parents=True, exist_ok=True)

W, H = 1200, 630
img = Image.new("RGB", (W, H), "#f7fbff")
draw = ImageDraw.Draw(img)

# Градиентный фон
for y in range(H):
    t = y / H
    r = int(247 * (1 - t) + 238 * t)
    g = int(251 * (1 - t) + 244 * t)
    b = int(255 * (1 - t) + 255 * t)
    draw.line([(0, y), (W, y)], fill=(r, g, b))

# Мягкие световые пятна
draw.ellipse((-120, -160, 520, 460), fill=(226, 234, 255))
draw.ellipse((760, -180, 1380, 440), fill=(221, 248, 255))
draw.ellipse((780, 260, 1320, 820), fill=(235, 226, 255))

# Карточка
card = (86, 76, 1114, 554)
draw.rounded_rectangle(card, radius=42, fill=(255, 255, 255), outline=(225, 231, 245), width=2)

# Лого-кружок робота
draw.rounded_rectangle((136, 128, 230, 222), radius=28, fill=(17, 25, 54))
draw.ellipse((164, 160, 178, 174), fill=(57, 223, 247))
draw.ellipse((188, 160, 202, 174), fill=(57, 223, 247))
draw.line((183, 124, 183, 104), fill=(83, 102, 255), width=5)
draw.ellipse((174, 94, 192, 112), fill=(83, 102, 255))

# Шрифты
def font(size, bold=False):
    paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    ]
    for p in paths:
        if Path(p).exists():
            return ImageFont.truetype(p, size=size)
    return ImageFont.load_default()

f_logo = font(56, True)
f_title = font(62, True)
f_text = font(30, False)
f_badge = font(24, True)

# ViMi
draw.text((252, 137), "ViMi", font=f_logo, fill=(41, 151, 255))

# Badge
draw.rounded_rectangle((136, 264, 418, 314), radius=25, fill=(239, 242, 255))
draw.text((160, 276), "Telegram SaaS", font=f_badge, fill=(83, 102, 255))

# Заголовок
draw.text((136, 340), "Автоматизация", font=f_title, fill=(17, 25, 54))
draw.text((136, 410), "Telegram-каналов", font=f_title, fill=(116, 83, 246))

# Описание
draw.text((136, 492), "Автопостинг · Видео · Очереди · Монетизация", font=f_text, fill=(86, 101, 139))

# Кнопка справа
draw.rounded_rectangle((790, 400, 1038, 474), radius=22, fill=(83, 102, 255))
draw.text((842, 420), "Open bot", font=font(30, True), fill=(255, 255, 255))

img.save(OUT, optimize=True)
print(f"created {OUT}")
