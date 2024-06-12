from database.db import database_connection
from managers.dce_report import fetch_report_dce
from functions.compare_dataframes import compare_df_with_dce_report
from utils.df_to_excel import to_excel
import os
import pandas as pd

def main():
    print ("Iniciando proceso de extracción de datos de Registro Civil para DCE...")

    # Variables de la ruta
    folder = 'history/abril_2024'
    folder_output = 'history/marzo_2024/output'
    path_binary_report = f'{folder}/report_bdd.xlsx'
    # Ruta del archivo de datos
    path_data = 'data.xlsx'
    path_compare_binary_report = f'{folder}/compare_binary_report.xlsx'
    path_output = f'{folder_output}/dce.xlsx'

    # Setear la fecha de inicio y fin
    start_date = '2024-04-01'
    end_date = '2024-04-31'

    # Crear instancia de database
    engine = database_connection()

    # Instanciar los dataframes vacios 
    df_binary_report = None

    print ("\n ============================================================= \n")

    # Obtener los datos de binary_report
    if not os.path.exists(path_binary_report):
        print(f"Archivo {path_binary_report} no existe")
        print ("Obteniendo datos de la base de datos...")
        # Obtener los datos de la base de datos
        df_binary_report = fetch_report_dce(engine, start_date, end_date)
        # Guardar el dataframe en un archivo excel
        to_excel(df_binary_report, folder, path_binary_report)
        print ("Archivo guardado exitosamente")
    else:
        print(f"Archivo {path_binary_report} ya existe")
        print ("Leyendo archivo existente...")
        df_binary_report = pd.read_excel(path_binary_report)
        print ("Archivo leido exitosamente")

    print ("\n ============================================================= \n")
    print ("Proceso de extracción de datos de Registro Civil para DCE finalizado")
    print ("\n ============================================================= \n")

    print ("Inicia proceso de comparación de datos...")
    print ("\n ============================================================= \n")

    print ("Llenar datos nulos con none en los dataframes...")
    df_binary_report.fillna('None', inplace=True)
    print ("\n ============================================================= \n")
    
    print ("Leyendo archivo ha comparar...")
    df_data = pd.read_excel(path_data)
    print ("Archivo leido exitosamente")

    print ("\n ============================================================= \n")
    
    if not os.path.exists(path_compare_binary_report):
        print ("Comparando datos en binary_report...")
        df_data = compare_df_with_dce_report(df_binary_report, df_data)
        print ("Datos comparados exitosamente")
        to_excel(df_data, folder_output, path_compare_binary_report)
    else:
        print ("Archivo compare_binary_report ya existe")
        print ("Leyendo archivo existente...")
        df_data = pd.read_excel(path_compare_binary_report)
        print ("Archivo leido exitosamente")
    

    print ("\n ============================================================= \n")
    print ("Proceso de comparación de datos finalizado")
    print ("\n ============================================================= \n")

    print ("Generando archivo final...")
    to_excel(df_data, folder_output, path_output)
    print ("Archivo final generado exitosamente")

    print ("\n ============================================================= \n")
    print ("Proceso finalizado")
    print ("\n ============================================================= \n")

    
if __name__ == "__main__":
    main()