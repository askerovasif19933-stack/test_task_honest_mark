from config import host, port, password, user
import psycopg2


class ObjectDataBaseConnect:
    """ класс для соединения, выборки, вставки и измения данных,
        атоматический commit при выходе из контекстсного менджера, 
        закрытие соединения с БД
        и rollback при возникновении исключения"""

    def __init__(self, base_name):
        self.connect_db= psycopg2.connect(
            user = user,
            host = host,
            port = port,
            password = password,
            database = base_name
        ) 
    
    def select(self, sql, parms=None, fetch_all=False):
        """Выборка множесмтвеная или единичная"""
        with self.connect_db.cursor() as cur:

            cur.execute(sql, parms)
            if fetch_all:
                return cur.fetchall()
            return cur.fetchone()
            
    
    def execute(self, sql, parms= None, execute_many = False):
        """Изменение и вставка данных"""

        with self.connect_db.cursor() as cur:
            if execute_many:
                cur.executemany(sql, parms)
            else:
                cur.execute(sql, parms)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.connect_db.rollback()
        else:
            self.connect_db.commit()
        self.connect_db.close()

        return False

        
        

    
            

        
        
