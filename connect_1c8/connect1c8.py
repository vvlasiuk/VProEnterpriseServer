import pythoncom
import win32com.client
from dotenv import load_dotenv
import os

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

def main():
    pythoncom.CoInitialize()
    # Створення COM-з'єднання з 1С

    connection_string = f'Srvr={os.getenv("C1_SRVR")};Ref={os.getenv("C1_REF")};Usr={os.getenv("C1_USR")};Pwd={os.getenv("C1_PWD")};'
    v8 = win32com.client.Dispatch("V83.COMConnector").Connect(connection_string)

    # Виконання процедури
    procedure_name = "ПСТ_ВідправкаПовідомлень.TestPython()"  # Наприклад: "ОновитиДокументи"
    rez = v8.ПСТ_ВідправкаПовідомлень.TestPython()
    print(rez.Текст)

    # Спробуємо CoUninitialize для цього циклу (щоб потік не залишався в неконсистентному стані)
    try:
        pythoncom.CoUninitialize()
    except Exception:
        pass

if __name__ == "__main__":
    main()