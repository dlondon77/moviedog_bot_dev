# test_card.py
import os
from PIL import Image, ImageDraw, ImageFont

# ==================== СОЗДАЕМ ТЕСТОВУЮ КАРТОЧКУ ====================

def create_test_card():
    """Создает тестовую карточку с одной картинкой"""
    
    # Путь к картинке (возьми любую из frames)
    frame_path = "/app/frames/test_frame_1.jpg"
    
    if not os.path.exists(frame_path):
        print(f"❌ Файл не найден: {frame_path}")
        print("Сначала отправь фото боту, чтобы оно сохранилось в frames/")
        return
    
    # Открываем изображение
    img = Image.open(frame_path)
    img = img.resize((1080, 1080))
    img = img.convert('RGB')
    
    # Создаем затемнение
    overlay = Image.new('RGBA', (1080, 1080), (0, 0, 0, 0))
    draw_overlay = ImageDraw.Draw(overlay)
    
    # Градиент сверху
    for y in range(350):
        alpha = int(200 * (1 - y / 350))
        draw_overlay.rectangle([(0, y), (1080, y+1)], fill=(0, 0, 0, alpha))
    
    # Градиент снизу
    for y in range(650, 1080):
        alpha = int(200 * ((y - 650) / 430))
        draw_overlay.rectangle([(0, y), (1080, y+1)], fill=(0, 0, 0, alpha))
    
    # Накладываем затемнение
    img = Image.alpha_composite(img.convert('RGBA'), overlay)
    img = img.convert('RGB')
    
    draw = ImageDraw.Draw(img)
    
    # Шрифты
    try:
        font_big = ImageFont.truetype('/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf', 60)
        font_title = ImageFont.truetype('/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf', 45)
        font_text = ImageFont.truetype('/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf', 35)
        font_small = ImageFont.truetype('/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf', 28)
    except:
        font_big = ImageFont.load_default()
        font_title = ImageFont.load_default()
        font_text = ImageFont.load_default()
        font_small = ImageFont.load_default()
    
    # ========== ВЕРХНЯЯ ЧАСТЬ ==========
    y = 50
    
    # Название фильма
    draw.text((50, y), "ТЕСТОВЫЙ ФИЛЬМ (2024)", fill=(255, 215, 0), font=font_big)
    y += 75
    
    # Страна
    draw.text((50, y), "🌍 США, 2024", fill=(255, 255, 255), font=font_small)
    y += 40
    
    # Режиссёр
    draw.text((50, y), "🎭 Режиссёр: Иван Иванов", fill=(200, 200, 200), font=font_small)
    y += 35
    
    # Актеры
    draw.text((50, y), "👥 Актеры: Петр Петров, Анна Сидорова", fill=(200, 200, 200), font=font_small)
    
    # Стрелка
    draw.text((1000, 30), "➜", fill=(255, 255, 255), font=font_big)
    
    # ========== ЦЕНТР ==========
    center_y = 400
    
    # Заголовок слайда
    title = "О чём лай?"
    bbox = draw.textbbox((0, 0), title, font=font_title)
    title_width = bbox[2] - bbox[0]
    draw.text(((1080 - title_width) // 2, center_y), title, fill=(255, 215, 0), font=font_title)
    
    # Текст слайда
    text_lines = [
        "Это тестовый текст для слайда.",
        "Он должен быть красивым и читаемым.",
        "Проверяем дизайн карточки."
    ]
    
    text_y = center_y + 70
    for line in text_lines:
        bbox = draw.textbbox((0, 0), line, font=font_text)
        text_width = bbox[2] - bbox[0]
        draw.text(((1080 - text_width) // 2, text_y), line, fill=(255, 255, 255), font=font_text)
        text_y += 55
    
    # ========== НИЖНЯЯ ЧАСТЬ ==========
    bottom_y = 920
    
    # Рубрика
    draw.text((50, bottom_y), "🐕 Мнение КиноИщейки", fill=(214, 94, 125), font=font_title)
    
    # Оценка
    rating = 9
    draw.text((950, bottom_y - 5), f"{rating}/10", fill=(255, 215, 0), font=font_big)
    
    # Косточки
    bones = ""
    for i in range(10):
        if i < rating:
            bones += "🦴"
        else:
            bones += "🫥"
    
    bbox = draw.textbbox((0, 0), bones, font=font_small)
    bones_width = bbox[2] - bbox[0]
    draw.text((1080 - bones_width - 50, bottom_y + 50), bones, fill=(214, 94, 125), font=font_small)
    
    # Хэштеги
    hashtags = "#Тест #Кино #Обзор"
    bbox = draw.textbbox((0, 0), hashtags, font=font_small)
    text_width = bbox[2] - bbox[0]
    draw.text(((1080 - text_width) // 2, 1030), hashtags, fill=(64, 167, 227), font=font_small)
    
    # Сохраняем
    output_path = "/app/output/test_card.png"
    img.save(output_path, 'PNG', quality=95)
    print(f"✅ Карточка создана: {output_path}")
    return output_path

if __name__ == "__main__":
    create_test_card()
