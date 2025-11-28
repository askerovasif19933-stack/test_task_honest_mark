
from venv import logger
from create_base import new_base
from object_database_connect import ObjectDataBaseConnect
from data_filler import make_data, make_documents
from logger import get_logger
# создаем таблицы и заполняем сгенерированными значениями

logger = get_logger(__name__)

# данные для базы:
data = make_data()
data_tbl = list(data.values())
documents_tbl = make_documents(data)


def create_table(base: str):
    """Создаем таблицы"""
    try:
        with ObjectDataBaseConnect(base) as db:
        
            sql_data = """
                CREATE TABLE IF NOT EXISTS public.data
                (
                    object character varying(50) COLLATE pg_catalog."default" NOT NULL,
                    status integer,
                    level integer,
                    parent character varying COLLATE pg_catalog."default",
                    owner character varying(14) COLLATE pg_catalog."default",
                    CONSTRAINT data_pkey PRIMARY KEY (object)
                )
            """
            
            sql_documents = """
                CREATE TABLE IF NOT EXISTS public.documents
                (
                    doc_id character varying COLLATE pg_catalog."default" NOT NULL,
                    recieved_at timestamp without time zone,
                    document_type character varying COLLATE pg_catalog."default",
                    document_data jsonb,
                    processed_at timestamp without time zone,
                    CONSTRAINT documents_pkey PRIMARY KEY (doc_id)
                )
            """

            db.execute(sql_data)
            db.execute(sql_documents)

            logger.info('Таблицы созданы')
            insert(db, data_tbl, documents_tbl)
    except Exception as e:
        logger.error(f'Ошибка {e}')



def insert(db: 'ObjectDataBaseConnect', data: list[dict], document: list[dict]):
    """Вставка сгенерированых сзначений в таблицу"""

    insert_data = [(i['object'], i['status'], i['level'], i['parent'], i['owner'])  for i in data]
    insert_doc = [(i['doc_id'], i['recieved_at'], i['document_type'], i['document_data'])  for i in document]
    sql_data = """
        INSERT INTO data(object, status, level, parent, owner)
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (object) DO NOTHING
        """
    sql_documents = """
        INSERT INTO documents(doc_id, recieved_at, document_type, document_data)
        VALUES (%s, %s, %s, %s) ON CONFLICT (doc_id) DO NOTHING
        """

    db.execute(sql_data, insert_data, execute_many=True)
    db.execute(sql_documents, insert_doc, execute_many=True)
 
    logger.info('Данные вставлены')





if __name__ == '__main__':
    create_table(new_base)