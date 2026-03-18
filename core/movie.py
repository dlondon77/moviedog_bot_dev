# core/movie.py
import sqlite3
import logging
import random
from datetime import datetime, timedelta
from core import db

logger = logging.getLogger('core.movie')

# ==================== ПОИСК ФИЛЬМОВ ====================

def search_movies_in_db(query: str, min_rating: float = 0.0, max_rating: float = 10.0) -> list:
    """Надежный поиск фильмов по названию"""
    conn = db.get_movies_db_connection()
    try:
        query_clean = db.clean_text(query, for_sql=True).strip()
        if not query_clean:
            return []

        # Разбиваем запрос на слова
        words = query_clean.split()
        
        # Ограничиваем количество слов для генерации вариантов (максимум 3)
        if len(words) > 3:
            words = words[:3]
        
        # Генерируем варианты для каждого слова
        word_variants = []
        for word in words:
            variants = [
                word,                  # оригинальный регистр
                word.lower(),          # все маленькие
                word.capitalize(),     # первая заглавная
                word.upper()           # все заглавные
            ]
            word_variants.append(list(set(variants)))  # удаляем дубликаты

        # Генерируем все возможные комбинации вариантов слов
        from itertools import product
        query_variants = [' '.join(combo) for combo in product(*word_variants)]
        
        # Добавляем варианты для поиска (начинается с и содержит)
        variants = []
        for qv in query_variants:
            variants.extend([
                f"{qv}%",       # начинается с запроса
                f"%{qv}%",      # содержит запрос где-то внутри
            ])

        # Удаляем дубликаты
        variants = list(set(variants))

        # Формируем SQL-запрос с приоритетом для начинающихся с запроса
        sql = """
        SELECT id FROM movies
        WHERE (
            -- Варианты, где название начинается с запроса (высший приоритет)
            """ + " OR ".join([f"(name LIKE ? COLLATE NOCASE)"] * len(variants)) + """
        )
        AND rating BETWEEN ? AND ?
        ORDER BY
            CASE
                -- Максимальный приоритет: точное совпадение
                WHEN name = ? THEN 0
                -- Высокий приоритет: начинается с запроса
                """ + "\n".join([f"WHEN name LIKE ? COLLATE NOCASE THEN {i+1}" 
                               for i in range(len(variants))]) + """
                -- Низкий приоритет: содержит запрос
                ELSE """ + str(len(variants)+1) + """
            END,
            -- Внутри каждой группы сортируем по рейтингу
            rating DESC
        LIMIT 100
        """

        # Подготавливаем параметры для запроса
        exact_match = query_clean
        params = variants + [min_rating, max_rating, exact_match] + variants

        cursor = conn.cursor()
        cursor.execute(sql, params)
        
        return [get_movie_details(row[0]) for row in cursor.fetchall() if row[0]]
        
    except Exception as e:
        logger.error(f"Ошибка поиска: {e}")
        return []
    finally:
        conn.close()


def search_movies_by_person_in_db(query: str, min_rating: float = 0.0, max_rating: float = 10.0) -> list:
    """Улучшенный поиск по персонам"""
    conn = db.get_movies_db_connection()
    try:
        query_clean = db.clean_text(query, for_sql=True).strip()
        if not query_clean or len(query_clean) < 2:
            return []

        # Разбиваем запрос на слова
        search_terms = [term.strip() for term in query_clean.split() if term.strip()]
        
        # Формируем условия поиска в зависимости от количества слов
        if len(search_terms) == 1:
            # Поиск по одному слову - ищем в любом месте имени
            term = search_terms[0]
            patterns = [
                f"%{term.capitalize()}%",  # Ищем слово с заглавной буквы в любом месте
            ]
        else:
            # Поиск по нескольким словам - учитываем последовательность
            first_terms = [t.capitalize() for t in search_terms[:-1]]
            last_term = search_terms[-1].capitalize()
            
            # Шаблоны для поиска:
            patterns = [
                ' '.join(first_terms + [last_term]) + '%',  # "Мэрил Стр%"
                ' '.join(first_terms) + ' %' + last_term + '%',  # "Мэрил %Стр%"
                '% ' + ' '.join(first_terms) + ' %' + last_term + '%',  # "% Мэрил %Стр%"
            ]
        
        # Ищем персон, соответствующих шаблонам
        return search_person_matches(patterns, min_rating, max_rating)
        
    except Exception as e:
        logger.error(f"Ошибка поиска по персонам: {e}")
        return []
    finally:
        conn.close()


def search_person_matches(patterns: list, min_rating: float, max_rating: float) -> list:
    """Поиск персон по заданным шаблонам"""
    conn = db.get_movies_db_connection()
    try:
        # Создаем условия для поиска по актерам и режиссерам
        conditions = []
        params = []
        
        for pattern in patterns:
            conditions.append("""
                (EXISTS (
                    SELECT 1 FROM movie_actors ma 
                    JOIN actors a ON ma.actor_id = a.id 
                    WHERE ma.movie_id = m.id AND (
                        a.name LIKE ? OR 
                        a.enName LIKE ?
                    )
                ) OR EXISTS (
                    SELECT 1 FROM movie_directors md 
                    JOIN directors d ON md.director_id = d.id 
                    WHERE md.movie_id = m.id AND (
                        d.name LIKE ? OR 
                        d.enName LIKE ?
                    )
                ))
            """)
            params.extend([pattern]*4)
        
        where_clause = " OR ".join(conditions) if conditions else "1=0"
        
        sql = f"""
        SELECT DISTINCT m.id 
        FROM movies m
        WHERE ({where_clause})
        AND m.rating BETWEEN ? AND ?
        ORDER BY m.rating DESC
        LIMIT 100
        """
        
        params += [min_rating, max_rating]
        
        cursor = conn.cursor()
        cursor.execute(sql, params)
        
        return [get_movie_details(row[0]) for row in cursor.fetchall() if row[0]]
    finally:
        conn.close()


def get_movie_details(movie_id: int) -> dict:
    """Получение полной информации о фильме с актерами и режиссерами"""
    conn = db.get_movies_db_connection()
    cursor = conn.cursor()
    
    try:
        # Основная информация о фильме
        cursor.execute("SELECT * FROM movies WHERE id = ?", (movie_id,))
        row = cursor.fetchone()
        
        if not row:
            return None
            
        columns = [column[0] for column in cursor.description]
        movie = dict(zip(columns, row))
        
        # Жанры
        cursor.execute("SELECT genre FROM genres WHERE movie_id = ?", (movie_id,))
        movie['genres'] = [row[0] for row in cursor.fetchall()]
        
        # Страны
        cursor.execute("SELECT country FROM countries WHERE movie_id = ?", (movie_id,))
        movie['countries'] = [row[0] for row in cursor.fetchall()]
        
        # Актеры (первые 10)
        cursor.execute("""
        SELECT a.id, a.name, a.enName 
        FROM actors a
        JOIN movie_actors ma ON a.id = ma.actor_id
        WHERE ma.movie_id = ?
        LIMIT 10
        """, (movie_id,))
        movie['actors'] = [dict(zip(['id', 'name', 'enName'], row)) for row in cursor.fetchall()]
        
        # Режиссеры (все)
        cursor.execute("""
        SELECT d.id, d.name, d.enName 
        FROM directors d
        JOIN movie_directors md ON d.id = md.director_id
        WHERE md.movie_id = ?
        """, (movie_id,))
        movie['directors'] = [dict(zip(['id', 'name', 'enName'], row)) for row in cursor.fetchall()]
        
        return movie
    except Exception as e:
        logger.error(f"Ошибка получения деталей фильма (ID: {movie_id}): {e}")
        return None
    finally:
        conn.close()


def get_random_movie_from_db(min_rating: float = 7.0, max_rating: float = 10.0, is_new_only: bool = False) -> dict:
    """Получение случайного фильма из объединенного кэша"""
    conn = db.get_movies_db_connection()
    cursor = conn.cursor()
    
    try:
        # Решаем, из какого пула выбирать (80% - 7-10, 20% - новинки 5-7)
        use_new_releases = random.random() < 0.2
        
        if use_new_releases:
            # Пробуем найти новинки с рейтингом 5-7
            sql = """
            SELECT id FROM movies 
            WHERE rating >= 5 AND rating <= 7
            AND is_new_release = 1
            ORDER BY RANDOM() LIMIT 1
            """
            cursor.execute(sql)
            row = cursor.fetchone()
            
            if row:
                return get_movie_details(row[0])
        
        # Если не нашли новинок или не выбрали их, ищем в основном пуле 7-10
        sql = """
        SELECT id FROM movies 
        WHERE rating >= ? AND rating <= ?
        """
        params = [min_rating, max_rating]
        
        if is_new_only:
            sql += " AND is_new_release = 1"
        
        sql += " ORDER BY RANDOM() LIMIT 1"
        
        cursor.execute(sql, params)
        row = cursor.fetchone()
        
        if row:
            return get_movie_details(row[0])
            
        return None
            
    except Exception as e:
        logger.error(f"Ошибка получения случайного фильма: {e}")
        return None
    finally:
        conn.close()


def get_premier_movies_from_db() -> list:
    """Получение списка премьерных фильмов за последний месяц и будущих"""
    conn = db.get_movies_db_connection()
    cursor = conn.cursor()
    
    try:
        one_month_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        
        # Получаем только ID фильмов
        sql = """
        SELECT id FROM movies 
        WHERE is_new_release = 1 AND 
            (premiere_russia >= ? OR premiere_world >= ?)
        ORDER BY 
            COALESCE(premiere_russia, premiere_world) ASC,
            await_count DESC
        LIMIT 100
        """
        cursor.execute(sql, (one_month_ago, one_month_ago))
        movie_ids = [row[0] for row in cursor.fetchall()]
        
        # Получаем полные данные для каждого фильма
        movies_with_details = []
        for movie_id in movie_ids:
            movie_details = get_movie_details(movie_id)
            if movie_details:
                movies_with_details.append(movie_details)
        
        return movies_with_details
    except Exception as e:
        logger.error(f"Ошибка получения премьерных фильмов: {e}")
        return []
    finally:
        conn.close()

def format_movie_card(movie, is_premiers=False, query=None, is_person_search=False):
    """Форматирует карточку фильма для отправки пользователю"""
    if not movie or not isinstance(movie, dict):
        return None, None

    try:
        # Основные данные
        title = movie.get('name', 'Без названия') or 'отсутствует'
        year = str(movie.get('year', '')) if movie.get('year') else 'отсутствует'
        is_new = movie.get('is_new_release', False)
        movie_id = str(movie.get('id', '')) if movie.get('id') else ''
        
        year_display = f"({year}) 🆕" if is_new else f"({year})"
        
        # Тип фильма
        content_type = movie.get('movie_type', 'movie')
        type_mapping = {
            'movie': 'фильм',
            'tv-series': 'сериал',
            'mini-series': 'мини-сериал',
            'cartoon': 'мультфильм'
        }
        type_text = type_mapping.get(content_type, 'фильм')
        
        rating = round(movie.get('rating', 0), 1) if movie.get('rating') else "отсутствует"
        countries = ', '.join(movie.get('countries', [])) if movie.get('countries') else 'отсутствует'
        genres = ', '.join(movie.get('genres', [])) if movie.get('genres') else 'отсутствует'
        description = movie.get('description', 'отсутствует') or 'отсутствует'
        
        # Получаем всех режиссеров
        directors_list = []
        for director in movie.get('directors', []):
            director_name = director.get('name', '') or director.get('enName', '')
            director_id = director.get('id', '')
            
            if director_name:
                if is_person_search and director_id and query and query.lower() in director_name.lower():
                    director_url = f"https://www.kinopoisk.ru/name/{director_id}/"
                    director_name = f"<a href='{director_url}'>{director_name}</a>"
                directors_list.append(director_name)
        
        directors = ', '.join(directors_list) if directors_list else 'отсутствует'
        
        # Актеры (первые 10)
        actors_list = []
        for actor in movie.get('actors', [])[:10]:
            actor_name = actor.get('name', '') or actor.get('enName', '')
            actor_id = actor.get('id', '')
            
            if actor_name:
                if is_person_search and actor_id and query and query.lower() in actor_name.lower():
                    actor_url = f"https://www.kinopoisk.ru/name/{actor_id}/"
                    actor_name = f"<a href='{actor_url}'>{actor_name}</a>"
                actors_list.append(actor_name)
        
        actors = ', '.join(actors_list) if actors_list else 'отсутствует'
        
        # Премьеры для новинок
        premiere_info = ""
        if is_premiers or movie.get('is_new_release'):
            premiere_russia = movie.get('premiere_russia')
            premiere_world = movie.get('premiere_world')
            await_count = movie.get('await_count', 0)
            
            def format_premiere_date(date_str):
                if not date_str:
                    return 'отсутствует'
                try:
                    if 'T' in date_str:
                        date_part = date_str.split('T')[0]
                        return datetime.strptime(date_part, "%Y-%m-%d").strftime("%d.%m.%Y")
                    else:
                        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y")
                except Exception as e:
                    logger.error(f"Ошибка форматирования даты: {date_str}, {e}")
                    return 'отсутствует'
            
            premiere_info = (
                f"🎉 Премьера РФ: <b>{format_premiere_date(premiere_russia)}</b>\n"
                f"🌎 Премьера Мир: <b>{format_premiere_date(premiere_world)}</b>\n"
                f"👥 Ожидают: <b>{int(await_count) if await_count else 0}</b> чел.\n"
            )
        
        poster_url = movie.get('poster_url', f"https://st.kp.yandex.net/images/film_big/{movie_id}.jpg")
        kp_url = f"https://www.kinopoisk.ru/film/{movie_id}/" if movie_id else "https://www.kinopoisk.ru/"
     
        card = (
            f"<a href='{poster_url}'>🎬</a> <a href='{kp_url}'><b>{title}</b></a> {year_display}\n"
            f"📁 Тип: <b>{type_text}</b>\n"
            f"⭐ Рейтинг Кинопоиска: <b>{rating}</b>\n"
            f"🌍 Страна: <b>{countries}</b>\n"
            f"🎭 Жанр: <b>{genres}</b>\n"
            f"{premiere_info}"
            f"📝 Описание: <i>{description}</i>\n"
            f"🎥 Режиссер: <b>{directors}</b>\n"
            f"👥 Актеры: <b>{actors}</b>\n"
        )
        
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        keyboard = []
        
        if movie_id and year:
            callback_data = f"ai:{movie_id}:{year}"[:64]
            keyboard.append([InlineKeyboardButton("Мнение КиноИщейки", callback_data=callback_data)])
        
        return card, InlineKeyboardMarkup(keyboard) if keyboard else None
        
    except Exception as e:
        logger.error(f"Ошибка форматирования карточки фильма: {e}")
        return None, None

def search_movies_with_filters(query, filters=None, count_only=False):
    """
    Поиск фильмов с фильтрами, используя существующий search_movies_in_db
    
    filters = {
        'rating_range': '5-6', '6-7', '7-8', '8-9', '9-10', 'new' (без рейтинга)
        'decade': '1980s', '1990s', '2000s', '2010s', '2020s', 'pre1980'
    }
    
    Возвращает:
        Если count_only=True: (количество, есть_ли_ещё)
        Если count_only=False: список фильмов
    """
    # Сначала получаем все фильмы по поиску (без фильтров)
    all_movies = search_movies_in_db(query, min_rating=0.0, max_rating=10.0)
    
    if not all_movies:
        return (0, False) if count_only else []
    
    # Применяем фильтры
    filtered_movies = []
    
    for movie in all_movies:
        include = True
        rating = movie.get('rating')
        year = movie.get('year')
        
        if filters:
            # Фильтр по рейтингу
            if filters.get('rating_range'):
                if filters['rating_range'] == 'new':
                    # Новинки без рейтинга
                    if rating is not None and rating > 0:
                        include = False
                else:
                    rating_parts = filters['rating_range'].split('-')
                    if len(rating_parts) == 2:
                        min_r = float(rating_parts[0])
                        max_r = float(rating_parts[1])
                        if rating is None or rating < min_r or rating > max_r:
                            include = False
            
            # Фильтр по десятилетию
            if include and filters.get('decade'):
                if not year:
                    include = False
                elif filters['decade'] == 'pre1980':
                    if year >= 1980:
                        include = False
                elif filters['decade'] == '1980s':
                    if year < 1980 or year >= 1990:
                        include = False
                elif filters['decade'] == '1990s':
                    if year < 1990 or year >= 2000:
                        include = False
                elif filters['decade'] == '2000s':
                    if year < 2000 or year >= 2010:
                        include = False
                elif filters['decade'] == '2010s':
                    if year < 2010 or year >= 2020:
                        include = False
                elif filters['decade'] == '2020s':
                    if year < 2020:
                        include = False
        
        if include:
            filtered_movies.append(movie)
    
    if count_only:
        # Если получили ровно 100 фильмов, значит возможно есть ещё
        # (потому что search_movies_in_db ограничивает 100)
        has_more = (len(filtered_movies) >= 100)
        return (len(filtered_movies), has_more)
    else:
        return filtered_movies
        
def format_filter_keyboard(query, current_filters=None, total_count=0, has_more=False):
    """
    Создает клавиатуру с фильтрами для поиска
    Теперь с toggle-кнопками (повторное нажатие снимает фильтр)
    
    Args:
        query: поисковый запрос
        current_filters: текущие активные фильтры
        total_count: количество найденных фильмов
        has_more: есть ли еще фильмы сверх лимита
    """
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    
    if current_filters is None:
        current_filters = {}
    
    keyboard = []
    
    # Статистика
    if total_count > 0:
        if has_more:
            count_text = f">{total_count}"
        else:
            count_text = str(total_count)
            
        keyboard.append([InlineKeyboardButton(
            f"📊 Найдено: {count_text} фильмов", 
            callback_data="noop"
        )])
    
    # ===== ФИЛЬТР ПО РЕЙТИНГУ =====
    rating_row = []
    current_rating = current_filters.get('rating_range')
    
    rating_options = [
        ('new', '🆕 Новинки'),
        ('5-6', '⭐ 5-6'),
        ('6-7', '⭐ 6-7'),
        ('7-8', '⭐ 7-8'),
        ('8-9', '⭐ 8-9'),
        ('9-10', '⭐ 9-10')
    ]
    
    for value, label in rating_options:
        if current_rating == value:
            # Активный фильтр - показываем с галочкой
            rating_row.append(InlineKeyboardButton(
                f"✓ {label}", 
                callback_data=f"filter_toggle_rating_{value}_{query}"
            ))
        else:
            rating_row.append(InlineKeyboardButton(
                label, 
                callback_data=f"filter_toggle_rating_{value}_{query}"
            ))
    
    # Разбиваем на две строки для компактности
    if rating_row:
        keyboard.append(rating_row[:3])  # Первые 3 кнопки
        keyboard.append(rating_row[3:])   # Остальные
    
    # ===== ФИЛЬТР ПО ДЕСЯТИЛЕТИЯМ =====
    decade_row = []
    current_decade = current_filters.get('decade')
    
    decade_options = [
        ('pre1980', '📽 До 1980'),
        ('1980s', '📅 1980-е'),
        ('1990s', '📅 1990-е'),
        ('2000s', '📅 2000-е'),
        ('2010s', '📅 2010-е'),
        ('2020s', '📅 2020-е')
    ]
    
    for value, label in decade_options:
        if current_decade == value:
            decade_row.append(InlineKeyboardButton(
                f"✓ {label}", 
                callback_data=f"filter_toggle_decade_{value}_{query}"
            ))
        else:
            decade_row.append(InlineKeyboardButton(
                label, 
                callback_data=f"filter_toggle_decade_{value}_{query}"
            ))
    
    # Разбиваем на две строки
    if decade_row:
        keyboard.append(decade_row[:3])
        keyboard.append(decade_row[3:])
    
    # ===== КНОПКА ПОКАЗА РЕЗУЛЬТАТОВ =====
    if total_count > 0:
        if has_more:
            button_text = f"🎬 Показать первые {total_count}"
        else:
            button_text = f"🎬 Показать карточки ({total_count})"
            
        keyboard.append([InlineKeyboardButton(
            button_text, 
            callback_data=f"filter_show_results_{query}"
        )])
    
    # ===== КНОПКИ УПРАВЛЕНИЯ =====
    control_row = []
    
    # Кнопка сброса (если есть активные фильтры)
    if current_filters:
        control_row.append(InlineKeyboardButton(
            "🔄 Сбросить все", 
            callback_data=f"filter_reset_all_{query}"
        ))
    
    control_row.append(InlineKeyboardButton(
        "🆕 Новый поиск", 
        callback_data="new_search"
    ))
    
    if control_row:
        keyboard.append(control_row)
    
    return InlineKeyboardMarkup(keyboard)
