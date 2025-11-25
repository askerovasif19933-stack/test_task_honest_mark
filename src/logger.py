import logging

def get_logger(name: str, log_file: str = 'app.log', level=logging.INFO):
    """
    Возвращает настроенный логгер.
    - name: имя логгера (обычно __name__ модуля)
    - log_file: имя файла для логов
    - level: уровень логирования
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    

    if not logger.handlers:
        # Консольный вывод
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)

        # Файл
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setLevel(level)

        # Формат
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)


        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    
    return logger