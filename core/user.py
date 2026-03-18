# core/user.py
import sqlite3
import logging
from datetime import datetime, date, timedelta
from core import db

logger = logging.getLogger('core.user')

# ==================== РЕГИСТРАЦИЯ И ДАННЫЕ ПОЛЬЗОВАТЕЛЯ ====================

def register_user(user_id, username, first_name, last_name, platform='telegram'):
    """Регистрирует пользователя с указанием платформы"""
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    
    # Проверяем, есть ли уже пользователь
    cursor.execute('SELECT 1 FROM users WHERE user_id = ?', (user_id,))
    exists = cursor.fetchone()
    
    if not exists:
        # Добавляем пользователя
        cursor.execute('''
        INSERT INTO users (user_id, username, first_name, last_name, registered_at, platform)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, username, first_name, last_name, datetime.now().isoformat(), platform))
        
        # Создаем подписку на тариф "Щенячий азарт" (ID=1)
        start_date = datetime.now().isoformat()
        end_date = (datetime.now() + timedelta(days=365*100)).isoformat()
        
        cursor.execute('''
        INSERT INTO user_subscriptions (user_id, tariff_id, start_date, end_date)
        VALUES (?, 1, ?, ?)
        ''', (user_id, start_date, end_date))
        
        cursor.execute('''
        INSERT INTO subscription_history (user_id, tariff_id, start_date, end_date)
        VALUES (?, 1, ?, ?)
        ''', (user_id, start_date, end_date))
        
        today = date.today().isoformat()
        cursor.execute('''
        INSERT INTO user_statistics (user_id, date)
        VALUES (?, ?)
        ''', (user_id, today))
        
        conn.commit()
        logger.info(f"Зарегистрирован новый пользователь: {user_id} на платформе {platform}")
    else:
        # Если пользователь уже есть, но platform не указана (старые записи)
        cursor.execute('''
        UPDATE users SET platform = ? WHERE user_id = ? AND platform IS NULL
        ''', (platform, user_id))
        conn.commit()
    
    conn.close()

def get_user_limits(user_id):
    """Возвращает лимиты пользователя согласно его подписке"""
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    
    # Получаем текущую активную подписку пользователя
    cursor.execute('''
    SELECT tp.opinion_limit, tp.regeneration_limit, tp.custom_query_limit, 
           tp.custom_retry_limit, tp.kinopoisk_query_limit, tp.name,
           us.end_date
    FROM tariff_plans tp
    JOIN user_subscriptions us ON tp.id = us.tariff_id
    WHERE us.user_id = ? AND us.is_active = 1 AND us.end_date > ?
    ORDER BY us.start_date DESC
    LIMIT 1
    ''', (user_id, datetime.now().isoformat()))
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return {
            'opinion_limit': result[0],
            'regeneration_limit': result[1],
            'custom_query_limit': result[2],
            'custom_retry_limit': result[3],
            'kinopoisk_query_limit': result[4],
            'tariff_name': result[5],
            'tariff_end_date': result[6]
        }
    else:
        # Возвращаем значения по умолчанию, если подписка не найдена
        return {
            'opinion_limit': 5,
            'regeneration_limit': 2,
            'custom_query_limit': 10,
            'custom_retry_limit': 30,
            'kinopoisk_query_limit': 50,
            'tariff_name': 'Щенячий азарт',
            'tariff_end_date': (datetime.now() + timedelta(days=365*100)).isoformat()
        }


def get_user_stats(user_id, date_str=None):
    """Возвращает статистику использования за указанную дату"""
    if date_str is None:
        date_str = date.today().isoformat()
    
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
    SELECT opinion_count, regeneration_count, custom_query_count, 
           custom_retry_count, kinopoisk_query_count 
    FROM user_statistics 
    WHERE user_id = ? AND date = ?
    ''', (user_id, date_str))
    result = cursor.fetchone()
    
    if result:
        stats = {
            'opinion_count': result[0],
            'regeneration_count': result[1],
            'custom_query_count': result[2],
            'custom_retry_count': result[3],
            'kinopoisk_query_count': result[4]
        }
    else:
        # Если записи нет, создаем новую
        cursor.execute('''
        INSERT INTO user_statistics (user_id, date)
        VALUES (?, ?)
        ''', (user_id, date_str))
        conn.commit()
        stats = {
            'opinion_count': 0,
            'regeneration_count': 0,
            'custom_query_count': 0,
            'custom_retry_count': 0,
            'kinopoisk_query_count': 0
        }
    
    conn.close()
    return stats


def increment_stat_counter(user_id, counter_name):
    """Увеличивает счетчик статистики для пользователя"""
    today = date.today().isoformat()
    
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    
    # Проверяем, есть ли запись для сегодня
    cursor.execute('''
    SELECT 1 FROM user_statistics WHERE user_id = ? AND date = ?
    ''', (user_id, today))
    exists = cursor.fetchone()
    
    if not exists:
        # Создаем новую запись
        cursor.execute('''
        INSERT INTO user_statistics (user_id, date)
        VALUES (?, ?)
        ''', (user_id, today))
    
    # Обновляем счетчик
    cursor.execute(f'''
    UPDATE user_statistics 
    SET {counter_name} = {counter_name} + 1 
    WHERE user_id = ? AND date = ?
    ''', (user_id, today))
    
    conn.commit()
    conn.close()


def record_user_opinion(user_id, movie_id):
    """Записывает факт просмотра мнения пользователем"""
    conn = db.get_opinions_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO user_movie_opinions (user_id, movie_id, created_at)
    VALUES (?, ?, ?)
    ''', (user_id, movie_id, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def is_admin(user_id):
    """Проверяет, является ли пользователь админом"""
    from core.admin import get_admin_ids
    return user_id in get_admin_ids()
