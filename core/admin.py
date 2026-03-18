# core/admin.py
import logging
import configparser
from datetime import date, timedelta
from core import db
from core.user import get_user_limits
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

logger = logging.getLogger('core.admin')

def get_admin_ids():
    """Возвращает список ID администраторов из конфига"""
    # Читаем конфиг заново, чтобы всегда брать актуальные данные
    import configparser
    config = configparser.ConfigParser()
    config.read('/volume1/homes/Dima/tgbots/moviedog/dev/config/config.ini')
    
    admin_ids_str = config['Admin']['admin_ids']
    return [int(id.strip()) for id in admin_ids_str.split(',')]

def is_admin(user_id):
    """Проверяет, является ли пользователь админом"""
    return user_id in get_admin_ids()

def get_admin_menu():
    """Возвращает меню для админки"""
    return {
        'users': '👥 Управление пользователями',
        'movies': '🎬 Управление фильмами',
        'opinions': '💭 Управление мнениями',
        'feedback': '📝 Обращения',
        'stats': '📊 Статистика'
    }

def get_users_list(limit=20, offset=0):
    """
    Возвращает список пользователей с базовой информацией
    """
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT 
            u.user_id,
            u.username,
            u.first_name,
            u.last_name,
            u.registered_at,
            COALESCE(us.opinion_count, 0) as today_opinions,
            COALESCE(us.regeneration_count, 0) as today_regenerations
        FROM users u
        LEFT JOIN user_statistics us ON u.user_id = us.user_id AND us.date = date('now')
        ORDER BY u.registered_at DESC
        LIMIT ? OFFSET ?
    ''', (limit, offset))
    
    users = cursor.fetchall()
    conn.close()
    return users

def get_top_active_users(limit=10, days=7):
    """
    Возвращает топ пользователей:
    - если есть активные за период - показывает их
    - если нет - показывает последних зарегистрированных
    """
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    
    start_date = (date.today() - timedelta(days=days)).isoformat()
    
    # Пробуем получить активных за период
    cursor.execute('''
        SELECT 
            u.user_id,
            u.username,
            u.first_name,
            u.last_name,
            u.registered_at,
            SUM(COALESCE(us.opinion_count, 0)) as total_opinions,
            SUM(COALESCE(us.regeneration_count, 0)) as total_regenerations,
            SUM(COALESCE(us.custom_query_count, 0)) as total_searches,
            SUM(COALESCE(us.kinopoisk_query_count, 0)) as total_kp_views,
            (SUM(COALESCE(us.opinion_count, 0)) + 
             SUM(COALESCE(us.regeneration_count, 0)) + 
             SUM(COALESCE(us.custom_query_count, 0)) + 
             SUM(COALESCE(us.kinopoisk_query_count, 0))) as total_actions,
            MAX(us.date) as last_active_date,
            COUNT(DISTINCT us.date) as active_days,
            (SELECT COUNT(*) FROM user_movie_opinions WHERE user_id = u.user_id) as total_opinions_alltime,
            julianday('now') - julianday(u.registered_at) as days_since_reg
        FROM users u
        LEFT JOIN user_statistics us ON u.user_id = us.user_id AND us.date >= ?
        GROUP BY u.user_id
        HAVING total_actions > 0
        ORDER BY total_actions DESC
        LIMIT ?
    ''', (start_date, limit))
    
    active_users = cursor.fetchall()
    
    # Если активных нет или меньше limit, добираем последними зарегистрированными
    if len(active_users) < limit:
        existing_ids = [u[0] for u in active_users]
        
        if existing_ids:
            placeholders = ','.join(['?'] * len(existing_ids))
            cursor.execute(f'''
                SELECT 
                    u.user_id,
                    u.username,
                    u.first_name,
                    u.last_name,
                    u.registered_at,
                    0 as total_opinions,
                    0 as total_regenerations,
                    0 as total_searches,
                    0 as total_kp_views,
                    0 as total_actions,
                    NULL as last_active_date,
                    0 as active_days,
                    (SELECT COUNT(*) FROM user_movie_opinions WHERE user_id = u.user_id) as total_opinions_alltime,
                    julianday('now') - julianday(u.registered_at) as days_since_reg
                FROM users u
                WHERE u.user_id NOT IN ({placeholders})
                ORDER BY u.registered_at DESC
                LIMIT ?
            ''', existing_ids + [limit - len(active_users)])
        else:
            cursor.execute('''
                SELECT 
                    u.user_id,
                    u.username,
                    u.first_name,
                    u.last_name,
                    u.registered_at,
                    0 as total_opinions,
                    0 as total_regenerations,
                    0 as total_searches,
                    0 as total_kp_views,
                    0 as total_actions,
                    NULL as last_active_date,
                    0 as active_days,
                    (SELECT COUNT(*) FROM user_movie_opinions WHERE user_id = u.user_id) as total_opinions_alltime,
                    julianday('now') - julianday(u.registered_at) as days_since_reg
                FROM users u
                ORDER BY u.registered_at DESC
                LIMIT ?
            ''', [limit - len(active_users)])
        
        new_users = cursor.fetchall()
        active_users.extend(new_users)
    
    conn.close()
    return active_users[:limit]  # гарантируем ровно limit записей

def search_users(query: str, limit=20):
    """
    Поиск пользователей по ID, username или имени
    """
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    
    # Пробуем интерпретировать как ID
    try:
        user_id = int(query)
        cursor.execute('''
            SELECT 
                u.user_id,
                u.username,
                u.first_name,
                u.last_name,
                u.registered_at,
                COALESCE(us.opinion_count, 0) as today_opinions,
                COALESCE(us.regeneration_count, 0) as today_regenerations
            FROM users u
            LEFT JOIN user_statistics us ON u.user_id = us.user_id AND us.date = date('now')
            WHERE u.user_id = ?
        ''', (user_id,))
        users = cursor.fetchall()
        if users:
            conn.close()
            return users
    except ValueError:
        pass
    
    # Поиск по username или имени
    search_term = f"%{query}%"
    cursor.execute('''
        SELECT 
            u.user_id,
            u.username,
            u.first_name,
            u.last_name,
            u.registered_at,
            COALESCE(us.opinion_count, 0) as today_opinions,
            COALESCE(us.regeneration_count, 0) as today_regenerations
        FROM users u
        LEFT JOIN user_statistics us ON u.user_id = us.user_id AND us.date = date('now')
        WHERE 
            u.username LIKE ? OR 
            u.first_name LIKE ? OR 
            u.last_name LIKE ?
        ORDER BY u.registered_at DESC
        LIMIT ?
    ''', (search_term, search_term, search_term, limit))
    
    users = cursor.fetchall()
    conn.close()
    return users

def get_user_full_stats(user_id):
    """
    Возвращает полную статистику пользователя для детальной карточки
    """
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    
    # Основная информация
    cursor.execute('''
        SELECT 
            u.user_id,
            u.username,
            u.first_name,
            u.last_name,
            u.registered_at,
            (SELECT COUNT(*) FROM user_movie_opinions WHERE user_id = u.user_id) as total_opinions,
            (SELECT MAX(created_at) FROM user_movie_opinions WHERE user_id = u.user_id) as last_active
        FROM users u
        WHERE u.user_id = ?
    ''', (user_id,))
    
    user_info = cursor.fetchone()
    
    if not user_info:
        conn.close()
        return None
    
    # Получаем лимиты
    limits = get_user_limits(user_id)
    
    # Статистика за сегодня
    cursor.execute('''
        SELECT 
            opinion_count,
            regeneration_count,
            custom_query_count,
            kinopoisk_query_count
        FROM user_statistics
        WHERE user_id = ? AND date = date('now')
    ''', (user_id,))
    
    today_stats = cursor.fetchone() or (0, 0, 0, 0)
    
    # Статистика за последние 7 дней
    cursor.execute('''
        SELECT 
            date,
            opinion_count,
            regeneration_count,
            custom_query_count,
            kinopoisk_query_count
        FROM user_statistics
        WHERE user_id = ?
        ORDER BY date DESC
        LIMIT 7
    ''', (user_id,))
    
    weekly_stats = cursor.fetchall()
    
    # Находим самый активный день
    cursor.execute('''
        SELECT 
            date,
            opinion_count + regeneration_count + custom_query_count + kinopoisk_query_count as total
        FROM user_statistics
        WHERE user_id = ?
        ORDER BY total DESC
        LIMIT 1
    ''', (user_id,))
    
    best_day = cursor.fetchone()
    
    # Количество обращений пользователя
    cursor.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) as new,
            SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) as resolved
        FROM feedback
        WHERE user_id = ?
    ''', (user_id,))
    
    feedback_stats = cursor.fetchone() or (0, 0, 0)
    
    # Последние 5 просмотренных фильмов
    cursor.execute('''
        SELECT 
            movie_id,
            created_at
        FROM user_movie_opinions
        WHERE user_id = ?
        ORDER BY created_at DESC
        LIMIT 5
    ''', (user_id,))
    
    recent_movies_raw = cursor.fetchall()
    
    # Получаем названия фильмов
    recent_movies = []
    if recent_movies_raw:
        movies_conn = db.get_movies_db_connection()
        movies_cursor = movies_conn.cursor()
        
        for movie_id, created_at in recent_movies_raw:
            movies_cursor.execute("SELECT name FROM movies WHERE id = ?", (movie_id,))
            movie_result = movies_cursor.fetchone()
            movie_name = movie_result[0] if movie_result else f"ID {movie_id}"
            recent_movies.append((movie_id, movie_name, created_at))
        
        movies_conn.close()
    
    conn.close()
    
    return {
        'info': user_info,
        'limits': limits,
        'today_stats': today_stats,
        'weekly_stats': weekly_stats,
        'best_day': best_day,
        'feedback_stats': feedback_stats,
        'recent_movies': recent_movies
    }
    
def search_movies_admin(query: str, limit=20):
    """
    Поиск фильмов по названию для админки
    """
    conn = db.get_movies_db_connection()
    cursor = conn.cursor()
    
    search_term = f"%{query}%"
    cursor.execute('''
        SELECT 
            id,
            name,
            year,
            rating,
            movie_type,
            is_new_release,
            await_count
        FROM movies
        WHERE name LIKE ? OR enName LIKE ?
        ORDER BY 
            CASE 
                WHEN name LIKE ? THEN 1
                WHEN name LIKE ? THEN 2
                ELSE 3
            END,
            rating DESC
        LIMIT ?
    ''', (search_term, search_term, f"{query}%", f"%{query}%", limit))
    
    movies = cursor.fetchall()
    conn.close()
    return movies

def get_movie_admin_details(movie_id: int):
    """
    Получает детальную информацию о фильме для админки
    """
    conn = db.get_movies_db_connection()
    cursor = conn.cursor()
    
    # Основная информация
    cursor.execute('''
        SELECT 
            id, name, enName, year, description, rating,
            movie_type, poster_url, premiere_russia, premiere_world,
            await_count, is_new_release
        FROM movies
        WHERE id = ?
    ''', (movie_id,))
    
    movie = cursor.fetchone()
    
    if not movie:
        conn.close()
        return None
    
    # Жанры
    cursor.execute("SELECT genre FROM genres WHERE movie_id = ?", (movie_id,))
    genres = [row[0] for row in cursor.fetchall()]
    
    # Страны
    cursor.execute("SELECT country FROM countries WHERE movie_id = ?", (movie_id,))
    countries = [row[0] for row in cursor.fetchall()]
    
    # Актеры (первые 10)
    cursor.execute('''
        SELECT a.name, a.enName
        FROM actors a
        JOIN movie_actors ma ON a.id = ma.actor_id
        WHERE ma.movie_id = ?
        LIMIT 10
    ''', (movie_id,))
    actors = []
    for row in cursor.fetchall():
        name = row[0] or row[1]
        if name:
            actors.append(name)
    
    # Режиссеры
    cursor.execute('''
        SELECT d.name, d.enName
        FROM directors d
        JOIN movie_directors md ON d.id = md.director_id
        WHERE md.movie_id = ?
    ''', (movie_id,))
    directors = []
    for row in cursor.fetchall():
        name = row[0] or row[1]
        if name:
            directors.append(name)
    
    conn.close()
    
    return {
        'movie': movie,
        'genres': genres,
        'countries': countries,
        'actors': actors,
        'directors': directors
    }

def get_anniversary_movies(year=None, month=None, min_rating=7.5, limit=100):
    """
    Возвращает юбилейные фильмы для указанного месяца и года
    (DB-06, DB-07, DB-08)
    
    Фильм считается юбилейным, если:
    1. Его премьера (РФ или мир) была в указанном месяце
    2. С момента премьеры прошло >=20 лет
    3. Количество лет кратно 5 (юбилей)
    4. Рейтинг >= min_rating
    """
    from datetime import datetime
    
    # Если год/месяц не указаны, берем текущие
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month
    
    conn = db.get_movies_db_connection()
    cursor = conn.cursor()
    
    # Форматируем месяц для SQL (с ведущим нулем)
    month_str = f"{month:02d}"
    
    query = '''
        SELECT 
            id,
            name,
            year,
            premiere_russia,
            premiere_world,
            rating,
            movie_type,
            CASE 
                WHEN premiere_russia IS NOT NULL THEN 
                    ? - CAST(strftime('%Y', DATE(premiere_russia)) AS INTEGER)
                ELSE 
                    ? - CAST(strftime('%Y', DATE(premiere_world)) AS INTEGER)
            END as years_since_premiere
        FROM movies
        WHERE 
            rating >= ? 
            AND (
                (premiere_russia IS NOT NULL 
                 AND strftime('%m', DATE(premiere_russia)) = ?)
                OR
                (premiere_world IS NOT NULL 
                 AND strftime('%m', DATE(premiere_world)) = ?)
            )
    '''
    
    cursor.execute(query, (year, year, min_rating, month_str, month_str))
    all_movies = cursor.fetchall()
    
    # Фильтруем только юбилеи (кратные 5, начиная с 20 лет)
    result = []
    for movie in all_movies:
        (id, name, release_year, premiere_ru, premiere_world, 
         rating, movie_type, years_since) = movie
        
        if years_since >= 20 and years_since % 5 == 0:
            # Определяем, какая премьера была
            premiere_date = premiere_ru or premiere_world
            if premiere_date:
                premiere_date = premiere_date[:10]  # YYYY-MM-DD
            
            # Ссылка на Кинопоиск
            kp_url = f"https://www.kinopoisk.ru/film/{id}/"
            
            result.append({
                'id': id,
                'name': name,
                'release_year': release_year,
                'premiere_date': premiere_date,
                'years_since': years_since,
                'rating': rating,
                'movie_type': movie_type,
                'kp_url': kp_url
            })
    
    # Сортируем: сначала по возрасту (от новых к старым), потом по рейтингу
    result.sort(key=lambda x: (-x['years_since'], -x['rating'] if x['rating'] else 0))
    
    conn.close()
    return result[:limit]  # Ограничиваем топ-100

def get_upcoming_premieres(year=None, month=None, limit=10):
    """
    Возвращает топ-10 ожидаемых новинок для указанного месяца
    (DB-09, DB-10, DB-11)
    
    Фильм считается новинкой, если его премьера в России или мире
    приходится на указанный месяц
    """
    from datetime import datetime, timedelta
    
    # Если год/месяц не указаны, берем текущие
    if year is None:
        year = datetime.now().year
    if month is None:
        month = datetime.now().month
    
    # Формируем начало и конец месяца
    month_start = f"{year}-{month:02d}-01"
    if month == 12:
        month_end = f"{year+1}-01-01"
    else:
        month_end = f"{year}-{month+1:02d}-01"
    
    conn = db.get_movies_db_connection()
    cursor = conn.cursor()
    
    # Ищем фильмы с премьерами в указанном месяце
    cursor.execute('''
        SELECT 
            id, name, year, rating, await_count,
            premiere_russia, premiere_world
        FROM movies
        WHERE is_new_release = 1
        AND (
            (premiere_russia >= ? AND premiere_russia < ?) OR
            (premiere_world >= ? AND premiere_world < ?)
        )
        ORDER BY await_count DESC
        LIMIT ?
    ''', (month_start, month_end, month_start, month_end, limit))
    
    movies = cursor.fetchall()
    conn.close()
    
    result = []
    for movie in movies:
        id, name, year, rating, await_count, premiere_ru, premiere_world = movie
        
        # Определяем дату премьеры для отображения
        premiere_date = premiere_ru or premiere_world
        if premiere_date:
            premiere_date = premiere_date[:10]  # оставляем только YYYY-MM-DD
        
        result.append({
            'id': id,
            'name': name,
            'year': year,
            'rating': rating,
            'await_count': await_count,
            'premiere_date': premiere_date,
            'premiere_russia': premiere_ru,
            'premiere_world': premiere_world
        })
    
    return result

def generate_anniversary_csv(movies, year, month):
    """
    Генерирует CSV-файл со списком юбилейных фильмов
    Возвращает путь к временному файлу
    """
    import csv
    import tempfile
    import os
    
    month_names = {
        1: "январь", 2: "февраль", 3: "март", 4: "апрель",
        5: "май", 6: "июнь", 7: "июль", 8: "август",
        9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь"
    }
    
    # Создаем временный файл
    fd, temp_path = tempfile.mkstemp(suffix='.csv', prefix=f'anniversary_{year}_{month}_', text=True)
    os.close(fd)
    
    with open(temp_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        
        # Заголовок
        writer.writerow(['ID', 'Название', 'Год выпуска', 'Дата премьеры', 
                        'Возраст (лет)', 'Рейтинг КП', 'Ссылка на Кинопоиск'])
        
        # Сортируем по возрасту (от старых к новым)
        movies_sorted = sorted(movies, key=lambda x: (-x['years_since'], -x['rating'] if x['rating'] else 0))
        
        for movie in movies_sorted:
            writer.writerow([
                movie['id'],
                movie['name'],
                movie['release_year'],
                movie['premiere_date'],
                movie['years_since'],
                f"{movie['rating']:.1f}" if movie['rating'] else 'нет',
                movie['kp_url']
            ])
    
    return temp_path

def get_feedback_list(status=None, limit=20, offset=0):
    """
    Возвращает список обращений с возможностью фильтрации по статусу
    """
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    
    query = '''
        SELECT 
            f.id, f.user_id, u.username, u.first_name,
            f.type, f.movie_id, f.message, f.status, f.admin_comment,
            f.created_at
        FROM feedback f
        JOIN users u ON f.user_id = u.user_id
    '''
    params = []
    
    if status:
        query += " WHERE f.status = ?"
        params.append(status)
    
    query += " ORDER BY f.created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    cursor.execute(query, params)
    feedback = cursor.fetchall()
    conn.close()
    return feedback

def get_feedback_count(status=None):
    """
    Возвращает количество обращений с возможностью фильтрации по статусу
    """
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    
    query = "SELECT COUNT(*) FROM feedback"
    params = []
    
    if status:
        query += " WHERE status = ?"
        params.append(status)
    
    cursor.execute(query, params)
    count = cursor.fetchone()[0]
    conn.close()
    return count

def get_feedback_by_id(feedback_id):
    """
    Возвращает детальную информацию об обращении по ID
    """
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT 
            f.id, f.user_id, u.username, u.first_name, u.last_name,
            f.type, f.movie_id, f.message, f.status, f.admin_comment,
            f.created_at
        FROM feedback f
        JOIN users u ON f.user_id = u.user_id
        WHERE f.id = ?
    ''', (feedback_id,))
    
    feedback = cursor.fetchone()
    conn.close()
    return feedback

def get_feedback_by_user_id(user_id, limit=20):
    """Возвращает обращения пользователя по его ID"""
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT 
            f.id, f.type, f.message, f.status, f.created_at
        FROM feedback f
        WHERE f.user_id = ?
        ORDER BY f.created_at DESC
        LIMIT ?
    ''', (user_id, limit))
    
    feedback = cursor.fetchall()
    conn.close()
    return feedback

def update_feedback_status(feedback_id, status, admin_comment=None):
    """
    Обновляет статус обращения и добавляет комментарий
    """
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    
    if admin_comment:
        cursor.execute('''
            UPDATE feedback 
            SET status = ?, admin_comment = ?
            WHERE id = ?
        ''', (status, admin_comment, feedback_id))
    else:
        cursor.execute('''
            UPDATE feedback 
            SET status = ?
            WHERE id = ?
        ''', (status, feedback_id))
    
    conn.commit()
    conn.close()
    return True

def get_feedback_stats():
    """
    Возвращает статистику по обращениям
    """
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) as new,
            SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress,
            SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) as resolved,
            SUM(CASE WHEN status = 'archive' THEN 1 ELSE 0 END) as archive
        FROM feedback
    ''')
    
    stats = cursor.fetchone()
    conn.close()
    return stats

def get_user_non_archived_feedback(user_id, limit=10):
    """Возвращает неархивные обращения пользователя"""
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT 
            f.id, f.type, f.message, f.status, f.created_at
        FROM feedback f
        WHERE f.user_id = ? AND f.status != 'archive'
        ORDER BY 
            CASE f.status
                WHEN 'new' THEN 1
                WHEN 'in_progress' THEN 2
                WHEN 'resolved' THEN 3
                ELSE 4
            END,
            f.created_at DESC
        LIMIT ?
    ''', (user_id, limit))
    
    feedback = cursor.fetchall()
    conn.close()
    return feedback
    
