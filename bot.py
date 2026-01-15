import os
import re
import zipfile
from io import BytesIO
import telebot
from telebot import types
import yt_dlp
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Токен бота
BOT_TOKEN = "8230005687:AAEGn2b0VT49kK9lGNGh9mRp09PEDT-cJtM"
BOT_USERNAME = "@scdownloaderru_bot"
bot = telebot.TeleBot(BOT_TOKEN)

# Глобальный словарь для хранения прогресса
progress_data = {}

def create_progress_bar(percentage, length=20):
    """Создание визуального прогресс-бара"""
    filled = int(length * percentage / 100)
    bar = '█' * filled + '░' * (length - filled)
    return f"[{bar}] {percentage:.1f}%"

def get_service_emoji(url):
    """Определение сервиса по URL и возврат эмодзи"""
    if 'soundcloud.com' in url or 'snd.sc' in url or 'on.soundcloud.com' in url:
        return '🟧', 'SoundCloud'
    elif 'music.yandex' in url or 'music.ya' in url:
        return '🟥', 'Яндекс Музыка'
    elif 'vk.com' in url or 'vk.ru' in url:
        return '🔵', 'ВКонтакте'
    elif 'spotify.com' in url or 'open.spotify' in url:
        return '🟢', 'Spotify'
    elif 'youtube.com' in url or 'youtu.be' in url:
        return '🔴', 'YouTube'
    elif 'deezer.com' in url:
        return '🟠', 'Deezer'
    else:
        return '🎵', 'Музыка'

def progress_hook(d, chat_id, message_id):
    """Хук для отслеживания прогресса скачивания"""
    if d['status'] == 'downloading':
        try:
            total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
            downloaded = d.get('downloaded_bytes', 0)
            
            if total > 0:
                percentage = (downloaded / total) * 100
                speed = d.get('speed', 0)
                eta = d.get('eta', 0)
                
                def format_bytes(bytes):
                    for unit in ['B', 'KB', 'MB', 'GB']:
                        if bytes < 1024:
                            return f"{bytes:.1f} {unit}"
                        bytes /= 1024
                    return f"{bytes:.1f} TB"
                
                speed_str = f"{format_bytes(speed)}/s" if speed else "--- KB/s"
                progress_bar = create_progress_bar(percentage)
                
                filename = d.get('filename', 'track').split('/')[-1]
                if len(filename) > 40:
                    filename = filename[:37] + "..."
                
                text = (
                    f"📥 *Скачивание*\n\n"
                    f"`{filename}`\n\n"
                    f"{progress_bar}\n\n"
                    f"📦 {format_bytes(downloaded)} / {format_bytes(total)}\n"
                    f"⚡ {speed_str}\n"
                    f"⏱ Осталось: {eta}s" if eta else ""
                )
                
                current_time = time.time()
                last_update = progress_data.get(f"{chat_id}_{message_id}", 0)
                
                if current_time - last_update > 2:
                    try:
                        bot.edit_message_text(
                            text,
                            chat_id,
                            message_id,
                            parse_mode='Markdown'
                        )
                        progress_data[f"{chat_id}_{message_id}"] = current_time
                    except:
                        pass
        except Exception as e:
            pass
    
    elif d['status'] == 'finished':
        try:
            bot.edit_message_text(
                "✅ Скачивание завершено!\n⚙️ Конвертирую в MP3...",
                chat_id,
                message_id
            )
        except:
            pass

def get_ydl_opts(output_path='%(title)s.%(ext)s', chat_id=None, message_id=None):
    """Настройки для yt-dlp"""
    opts = {
        'format': 'bestaudio/best',
        'outtmpl': output_path,
        'quiet': True,
        'no_warnings': True,
        'extract_flat': False,
        'cookiefile': None,
        'nocheckcertificate': True,
        'concurrent_fragment_downloads': 3,
        'retries': 15,
        'fragment_retries': 15,
        'skip_unavailable_fragments': False,
        'buffersize': 1024 * 512,
        'http_chunk_size': 1024 * 1024 * 5,
        'socket_timeout': 30,
        'file_access_retries': 10,
        'extractor_retries': 5,
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '320',
        }],
        'postprocessor_args': [
            '-threads', '2',
        ],
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
        },
        'keepvideo': False,
        'no_check_certificate': True,
    }
    
    if chat_id and message_id:
        opts['progress_hooks'] = [lambda d: progress_hook(d, chat_id, message_id)]
    
    return opts

def is_music_url(url):
    """Проверка, является ли URL ссылкой на музыкальный сервис"""
    patterns = [
        r'(soundcloud\.com|snd\.sc|on\.soundcloud\.com)',
        r'(music\.yandex\.|music\.ya\.)',
        r'(vk\.com|vk\.ru)/(audio|music|wall)',
        r'(spotify\.com|open\.spotify\.com)',
        r'(youtube\.com|youtu\.be)',
        r'deezer\.com'
    ]
    return any(re.search(pattern, url) for pattern in patterns)

def resolve_url(url):
    """Разворачивает короткие ссылки"""
    try:
        with yt_dlp.YoutubeDL({'quiet': True}) as ydl:
            info = ydl.extract_info(url, download=False)
            return info.get('webpage_url', url)
    except:
        return url

def is_playlist_or_album(url):
    """Проверка, является ли URL плейлистом или альбомом"""
    playlist_indicators = [
        '/sets/', '/albums/', '/playlist', 
        'album/', '&list=', '/playlists/'
    ]
    return any(indicator in url for indicator in playlist_indicators)

def create_main_keyboard():
    """Создание главной клавиатуры"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    
    buttons = [
        types.InlineKeyboardButton("🟧 SoundCloud", callback_data="help_sc"),
        types.InlineKeyboardButton("🟥 Яндекс Музыка", callback_data="help_ya"),
        types.InlineKeyboardButton("🔵 ВКонтакте", callback_data="help_vk"),
        types.InlineKeyboardButton("🟢 Spotify", callback_data="help_sp"),
        types.InlineKeyboardButton("🔴 YouTube", callback_data="help_yt"),
        types.InlineKeyboardButton("💡 Помощь", callback_data="help_main")
    ]
    
    markup.add(*buttons)
    return markup

@bot.callback_query_handler(func=lambda call: call.data.startswith('help_'))
def handle_help_callback(call):
    """Обработка нажатий на кнопки помощи"""
    help_texts = {
        'help_main': (
            "📖 *Инструкция по использованию*\n\n"
            "Просто отправь ссылку на:\n"
            "• Трек — получишь MP3\n"
            "• Плейлист/альбом — получишь ZIP архив\n\n"
            "🎵 *Поддерживаемые сервисы:*\n"
            "• SoundCloud\n"
            "• Яндекс Музыка\n"
            "• ВКонтакте Музыка\n"
            "• Spotify\n"
            "• YouTube\n"
            "• Deezer\n\n"
            "⚡ Качество: MP3 320kbps"
        ),
        'help_sc': (
            "🟧 *SoundCloud*\n\n"
            "Поддерживаются:\n"
            "✅ Треки\n"
            "✅ Плейлисты\n"
            "✅ Альбомы\n"
            "✅ Короткие ссылки on.soundcloud.com\n\n"
            "Примеры:\n"
            "`soundcloud.com/artist/track`\n"
            "`soundcloud.com/artist/sets/playlist`"
        ),
        'help_ya': (
            "🟥 *Яндекс Музыка*\n\n"
            "Поддерживаются:\n"
            "✅ Треки\n"
            "✅ Альбомы\n"
            "✅ Плейлисты\n\n"
            "Примеры:\n"
            "`music.yandex.ru/album/123/track/456`\n"
            "`music.yandex.ru/album/123`"
        ),
        'help_vk': (
            "🔵 *ВКонтакте*\n\n"
            "Поддерживаются:\n"
            "✅ Треки из аудио\n"
            "✅ Плейлисты\n\n"
            "⚠️ Может потребоваться авторизация\n\n"
            "Примеры:\n"
            "`vk.com/audio123_456`\n"
            "`vk.com/music/playlist/123_456`"
        ),
        'help_sp': (
            "🟢 *Spotify*\n\n"
            "Поддерживаются:\n"
            "✅ Треки\n"
            "✅ Альбомы\n"
            "✅ Плейлисты\n\n"
            "Примеры:\n"
            "`open.spotify.com/track/...`\n"
            "`open.spotify.com/album/...`\n"
            "`open.spotify.com/playlist/...`"
        ),
        'help_yt': (
            "🔴 *YouTube*\n\n"
            "Поддерживаются:\n"
            "✅ Видео (только аудио)\n"
            "✅ Плейлисты\n\n"
            "Примеры:\n"
            "`youtube.com/watch?v=...`\n"
            "`youtu.be/...`\n"
            "`youtube.com/playlist?list=...`"
        )
    }
    
    text = help_texts.get(call.data, help_texts['help_main'])
    
    try:
        bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            parse_mode='Markdown',
            reply_markup=create_main_keyboard()
        )
    except:
        pass
    
    bot.answer_callback_query(call.id)

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    welcome_text = (
        "🎵 *Multi-Service Music Downloader*\n\n"
        "Скачивай музыку из любых популярных сервисов!\n\n"
        "🎯 *Как использовать:*\n"
        "Просто отправь ссылку на трек или плейлист\n\n"
        "✨ *Возможности:*\n"
        "• MP3 320kbps качество\n"
        "• Поддержка плейлистов → ZIP\n"
        "• Прогресс-бар загрузки\n"
        "• Быстрая скорость\n\n"
        "Выбери сервис для подробной информации:"
    )
    
    bot.send_message(
        message.chat.id,
        welcome_text,
        parse_mode='Markdown',
        reply_markup=create_main_keyboard()
    )

@bot.message_handler(func=lambda message: True)
def handle_message(message):
    url = message.text.strip()
    
    if not is_music_url(url):
        bot.reply_to(
            message, 
            "❌ Не могу распознать ссылку.\n\n"
            "Поддерживаемые сервисы:\n"
            "🟧 SoundCloud\n"
            "🟥 Яндекс Музыка\n"
            "🔵 ВКонтакте\n"
            "🟢 Spotify\n"
            "🔴 YouTube\n\n"
            "Используй /help для помощи"
        )
        return
    
    emoji, service = get_service_emoji(url)
    status_msg = bot.reply_to(message, f"{emoji} Получаю информацию из {service}...")
    
    try:
        resolved_url = resolve_url(url)
        
        if is_playlist_or_album(resolved_url):
            download_playlist(message, resolved_url, status_msg, emoji, service)
        else:
            download_single_track(message, resolved_url, status_msg, emoji, service)
    except Exception as e:
        error_text = str(e)
        
        # Обработка различных ошибок
        if "Sign in to confirm you're not a bot" in error_text or "Sign in" in error_text:
            error_text = "⚠️ Требуется авторизация в сервисе."
        elif "Private" in error_text or "unavailable" in error_text:
            error_text = "🔒 Контент недоступен (приватный или удален)"
        elif "age-restricted" in error_text.lower():
            error_text = "🔞 Контент имеет возрастное ограничение"
        elif "copyright" in error_text.lower():
            error_text = "©️ Контент защищен авторским правом"
        elif "50" in error_text or "file size" in error_text.lower():
            error_text = "📦 Файл слишком большой (лимит 50MB)"
        else:
            # Обрезаем длинные ошибки
            error_text = f"❌ Ошибка: {error_text[:150]}"
        
        try:
            bot.edit_message_text(
                error_text,
                message.chat.id,
                status_msg.message_id
            )
        except:
            bot.send_message(
                message.chat.id,
                error_text
            )

def download_single_track(message, url, status_msg, emoji, service):
    """Скачивание одного трека"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            temp_dir = f"temp_{message.chat.id}"
            os.makedirs(temp_dir, exist_ok=True)
            
            ydl_opts = get_ydl_opts(
                f"{temp_dir}/%(title)s.%(ext)s",
                message.chat.id,
                status_msg.message_id
            )
            
            if retry_count > 0:
                bot.edit_message_text(
                    f"{emoji} Повторная попытка {retry_count}/{max_retries}...",
                    message.chat.id,
                    status_msg.message_id
                )
            else:
                bot.edit_message_text(
                    f"{emoji} Анализирую трек из {service}...",
                    message.chat.id,
                    status_msg.message_id
                )
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                title = info.get('title', 'track')
                duration = info.get('duration', 0)
                uploader = info.get('uploader', 'Unknown')
            
            files = [f for f in os.listdir(temp_dir) if f.endswith('.mp3')]
            if not files:
                raise Exception("Файл не был скачан")
            
            file_path = os.path.join(temp_dir, files[0])
            
            bot.edit_message_text(
                f"{emoji} Отправляю трек...",
                message.chat.id,
                status_msg.message_id
            )
            
            mins, secs = divmod(duration, 60)
            duration_str = f"{int(mins)}:{int(secs):02d}"
            
            with open(file_path, 'rb') as audio:
                # Проверяем размер файла (Telegram лимит 50MB для обычных ботов)
                file_size = os.path.getsize(file_path)
                
                if file_size > 50 * 1024 * 1024:  # Больше 50MB
                    bot.delete_message(message.chat.id, status_msg.message_id)
                    bot.send_message(
                        message.chat.id,
                        f"⚠️ *Файл слишком большой*\n\n"
                        f"Размер: {file_size / (1024*1024):.1f} MB\n"
                        f"Лимит Telegram: 50 MB\n\n"
                        f"Попробуйте скачать трек короче или используйте другой сервис.",
                        parse_mode='Markdown'
                    )
                    cleanup_temp_dir(temp_dir)
                    return
                
                try:
                    # Экранируем специальные символы Markdown
                    def escape_markdown(text):
                        if not text:
                            return "Unknown"
                        # Заменяем проблемные символы
                        escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
                        for char in escape_chars:
                            text = text.replace(char, f'\\{char}')
                        return text
                    
                    safe_title = escape_markdown(title[:50])
                    safe_uploader = escape_markdown(uploader[:30])
                    
                    bot.send_audio(
                        message.chat.id,
                        audio,
                        title=title[:64] if len(title) > 64 else title,
                        performer=uploader[:64] if len(uploader) > 64 else uploader,
                        duration=duration if duration and duration < 2147483647 else None,
                        caption=(
                            f"{emoji} {safe_title}{'\\.\\.\\.' if len(title) > 50 else ''}\n"
                            f"🤖 {BOT_USERNAME}"
                        ),
                        parse_mode='MarkdownV2',
                        timeout=120
                    )
                except Exception as send_error:
                    # Если не получилось отправить как аудио, отправляем как документ БЕЗ parse_mode
                    audio.seek(0)
                    bot.send_document(
                        message.chat.id,
                        audio,
                        visible_file_name=f"{title[:100]}.mp3",
                        caption=(
                            f"{emoji} {title[:50]}{'...' if len(title) > 50 else ''}\n"
                            f"🤖 {BOT_USERNAME}"
                        ),
                        timeout=120
                    )
            
            bot.delete_message(message.chat.id, status_msg.message_id)
            cleanup_temp_dir(temp_dir)
            break
            
        except Exception as e:
            retry_count += 1
            cleanup_temp_dir(temp_dir)
            
            if retry_count >= max_retries:
                raise Exception(f"Не удалось скачать после {max_retries} попыток: {str(e)}")
            
            time.sleep(2)

def download_playlist(message, url, status_msg, emoji, service):
    """Скачивание плейлиста/альбома"""
    try:
        temp_dir = f"temp_{message.chat.id}"
        os.makedirs(temp_dir, exist_ok=True)
        
        bot.edit_message_text(
            f"{emoji} Анализирую плейлист из {service}...",
            message.chat.id,
            status_msg.message_id
        )
        
        with yt_dlp.YoutubeDL({'quiet': True, 'extract_flat': True}) as ydl:
            playlist_info = ydl.extract_info(url, download=False)
            entries = playlist_info.get('entries', [])
            total_tracks = len(entries)
            playlist_title = playlist_info.get('title', 'playlist')
        
        bot.edit_message_text(
            f"{emoji} *Скачивание плейлиста*\n\n"
            f"`{playlist_title}`\n\n"
            f"📊 Треков: {total_tracks}\n"
            f"⚡ Параллельная загрузка активна!\n"
            f"⏳ Начинаю загрузку...",
            message.chat.id,
            status_msg.message_id,
            parse_mode='Markdown'
        )
        
        downloaded_count = [0]
        lock = threading.Lock()
        
        def download_single_entry(entry, index):
            try:
                entry_url = entry.get('url') or entry.get('webpage_url')
                if not entry_url:
                    return None
                
                ydl_opts = get_ydl_opts(f"{temp_dir}/{index:03d} - %(title)s.%(ext)s")
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([entry_url])
                
                with lock:
                    downloaded_count[0] += 1
                    try:
                        percentage = (downloaded_count[0] / total_tracks) * 100
                        progress_bar = create_progress_bar(percentage)
                        
                        bot.edit_message_text(
                            f"{emoji} *Скачивание плейлиста*\n\n"
                            f"`{playlist_title}`\n\n"
                            f"{progress_bar}\n\n"
                            f"✅ {downloaded_count[0]} / {total_tracks} треков\n"
                            f"⚡ Параллельная загрузка",
                            message.chat.id,
                            status_msg.message_id,
                            parse_mode='Markdown'
                        )
                    except:
                        pass
                
                return True
            except Exception as e:
                print(f"Ошибка скачивания трека {index}: {e}")
                return None
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(download_single_entry, entry, idx + 1)
                for idx, entry in enumerate(entries)
            ]
            
            for future in as_completed(futures):
                try:
                    future.result(timeout=180)
                except Exception as e:
                    print(f"Ошибка при загрузке трека: {e}")
        
        files = sorted([f for f in os.listdir(temp_dir) if f.endswith('.mp3')])
        
        if not files:
            raise Exception("Не удалось скачать треки")
        
        bot.edit_message_text(
            f"{emoji} *Упаковка архива*\n\n"
            f"⚙️ Создаю ZIP файл...\n"
            f"📊 Треков: {len(files)}",
            message.chat.id,
            status_msg.message_id,
            parse_mode='Markdown'
        )
        
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as zip_file:
            for idx, file in enumerate(files, 1):
                file_path = os.path.join(temp_dir, file)
                zip_file.write(file_path, file)
                
                if idx % 3 == 0:
                    try:
                        progress = (idx / len(files)) * 100
                        bar = create_progress_bar(progress)
                        bot.edit_message_text(
                            f"{emoji} *Упаковка архива*\n\n"
                            f"{bar}\n\n"
                            f"📊 {idx} / {len(files)} треков",
                            message.chat.id,
                            status_msg.message_id,
                            parse_mode='Markdown'
                        )
                    except:
                        pass
        
        zip_buffer.seek(0)
        zip_size = len(zip_buffer.getvalue())
        
        # Проверка размера ZIP файла
        if zip_size > 50 * 1024 * 1024:  # Больше 50MB
            bot.edit_message_text(
                f"⚠️ *Архив слишком большой*\n\n"
                f"Размер: {zip_size / (1024*1024):.1f} MB\n"
                f"Лимит Telegram: 50 MB\n\n"
                f"Попробуйте скачать меньший плейлист.",
                message.chat.id,
                status_msg.message_id,
                parse_mode='Markdown'
            )
            cleanup_temp_dir(temp_dir)
            return
        
        def format_size(bytes):
            for unit in ['B', 'KB', 'MB', 'GB']:
                if bytes < 1024:
                    return f"{bytes:.1f} {unit}"
                bytes /= 1024
            return f"{bytes:.1f} TB"
        
        bot.edit_message_text(
            f"{emoji} Отправляю архив...\n"
            f"💾 Размер: {format_size(zip_size)}",
            message.chat.id,
            status_msg.message_id
        )
        
        zip_filename = f"{playlist_title[:100]}.zip"  # Ограничение длины имени
        
        # Экранируем текст для Markdown
        def escape_markdown(text):
            if not text:
                return "Unknown"
            escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
            for char in escape_chars:
                text = text.replace(char, f'\\{char}')
            return text
        
        safe_playlist_title = escape_markdown(playlist_title[:50])
        
        bot.send_document(
            message.chat.id,
            zip_buffer,
            visible_file_name=zip_filename,
            caption=(
                f"{emoji} {safe_playlist_title}{'\\.\\.\\.' if len(playlist_title) > 50 else ''}\n\n"
                f"📦 Треков: {len(files)}\n"
                f"💾 Размер: {format_size(zip_size)}\n"
                f"🤖 {BOT_USERNAME}"
            ),
            parse_mode='MarkdownV2',
            timeout=180
        )
        
        bot.delete_message(message.chat.id, status_msg.message_id)
        cleanup_temp_dir(temp_dir)
        
    except Exception as e:
        raise e

def cleanup_temp_dir(temp_dir):
    """Удаление временной директории"""
    try:
        if os.path.exists(temp_dir):
            for file in os.listdir(temp_dir):
                os.remove(os.path.join(temp_dir, file))
            os.rmdir(temp_dir)
    except Exception as e:
        print(f"Ошибка при очистке: {e}")

if __name__ == "__main__":
    print("🤖 Multi-Service Music Downloader Bot запущен...")
    print(f"📱 Username: {BOT_USERNAME}")
    print("🎵 Поддерживаемые сервисы:")
    print("  🟧 SoundCloud")
    print("  🟥 Яндекс Музыка")
    print("  🔵 ВКонтакте")
    print("  🟢 Spotify")
    print("  🔴 YouTube")
    print("  🟠 Deezer")
    bot.infinity_polling()