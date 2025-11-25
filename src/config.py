from dotenv import load_dotenv
import os

load_dotenv()

# Параметры подключения
host= os.getenv('DB_HOST')
port= os.getenv('DB_PORT')
user= os.getenv('DB_USER')
password= os.getenv('DB_PASSWORD')