from object_database_connect import ObjectDataBaseConnect
from dal import process_single_document, indexing 
from create_base import new_base



        
def main(base: str):
    try:
        with ObjectDataBaseConnect(base) as db:
            # old_val = db.select("""SELECT status, owner from data""", fetch_all=True)
            indexing(db)
            while True:
                
                row = process_single_document(db)
                if not row:
                    print(f'Все документы обработаны')
                    break
            # new_val = db.select("""SELECT status, owner from data""", fetch_all=True)
            # for k,v in zip(old_val, new_val):
            #     print(k, k==v, v)
    except Exception as e:
        return False


    return True

if __name__ == '__main__':
    main(new_base)

