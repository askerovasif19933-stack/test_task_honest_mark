
from operator import le
from object_database_connect import ObjectDataBaseConnect
from logger import get_logger

logger = get_logger(__name__)

def indexing(db: 'ObjectDataBaseConnect'):
    """Индексация полей для быстрого доступа без блокировки таблиц (для больших таблиц)"""
    # Список индексов для создания 
    # CREATE INDEX CONCURRENTLY - если таблица очень большая не блокирует запись 
    sql = [
        # Таблица documents
        'CREATE INDEX IF NOT EXISTS idx_documents_3_fields ON documents(processed_at, document_type, recieved_at)',

        # Таблица data
        'CREATE INDEX IF NOT EXISTS idx_data_parent ON data USING HASH (parent)',
        'CREATE INDEX IF NOT EXISTS idx_data_status_owner ON data(status, owner)'
        ]
    
    # Выполняем каждый индекс отдельно
    for query in sql:
        db.execute(query)
    logger.info('Индексация полей завершина')
    


def select_one_doc(db: 'ObjectDataBaseConnect'):
    """SQL запрсо берет один не обработаный документ """
    sql = """
            SELECT doc_id, document_data FROM documents
            WHERE processed_at is NULL AND document_type = 'transfer_document'
            ORDER BY recieved_at ASC
            LIMIT 1      
            """
    row = db.select(sql)

    if row:
        logger.info(f'Выбран документ с id = {row[0]}')
    else:
        logger.info('Нет необработаных документов')

    return row


def parsing_data(row: tuple):
    """Разбираем картеж на doc_id, json, разбирам json на objects, operation_details"""
    doc_id, jsonb = row
    obj = jsonb['objects']
    operation_details = jsonb['operation_details']

    logger.debug(f'Документ {doc_id}, распарсен на {len(obj)} обьектов и {len(operation_details)} операций')

    return doc_id, obj, operation_details


def search_all_child(db: 'ObjectDataBaseConnect', parent: list):
    """Поиск дочерних объектов"""

    placeholders = ', '.join('%s' for i in range(len(parent)))

    sql = f"SELECT object FROM data WHERE parent IN ({placeholders})"

    child = set(row[0] for row in db.select(sql, tuple(parent), fetch_all=True))

    parent_child = list(child)
    parent_child.extend(parent)

    logger.debug(f'Найдено {len(child)} дочерних  для родительских {parent}')
    return parent_child


def correct_data(db: 'ObjectDataBaseConnect', all_parand_child: list, operation_details: dict[str, dict]):
    """Изменения старых значений на новые"""
    if operation_details:

        for operation, details in operation_details.items():
            new = details['new']
            old = details['old']

            db.execute(f""" 
                UPDATE data
                SET {operation} = %s
                WHERE {operation} = %s
                AND object = ANY(%s)
                """, (new, old, (all_parand_child,)))
            logger.info(f'Обнавлен поле {operation} с {old} на {new} для {len(all_parand_child)} обьектов')


def set_processing_time(db: 'ObjectDataBaseConnect', doc_id: str):
    """Установка даты и времени для обработаных документов"""
    sql = """
            UPDATE documents
            SET processed_at = NOW()
            WHERE doc_id = %s
            """

    db.execute(sql, (doc_id,))
    logger.info(f'Документ {doc_id} отмечен как обработанный')


def process_single_document(db:'ObjectDataBaseConnect'):
    """Обработка одного документа"""
    logger.info('Начинает обработку одного документа')
    row = select_one_doc(db)
    if not row:
        logger.info('Документов для обрабоатки нет')
        return None

    doc_id, object, operation_details = parsing_data(row)
    all_parand_child = search_all_child(db, object)

    correct_data(db, all_parand_child, operation_details)
    set_processing_time(db, doc_id)
    logger.info(f'Обработка докумнта {doc_id} завершина')

    return row
