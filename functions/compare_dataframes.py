from tqdm import tqdm
import pandas as pd
def compare_df_with_dce_report(df_binary_report, df_data):
    tqdm.pandas()
    # Asegúrate de que los valores sean cadenas
    df_binary_report['variables'] = df_binary_report['variables'].astype(str)
    def fix_dni(valor):
        valor = str(valor) # Asegura que el valor sea una cadena
        return '0' + valor if len(valor) == 9 else valor

    def safe_contains(series, value):
        value = str(value)# Convertir a cadena
        return series.str.contains(value, na=False)
    
    df_data['Cedula Usuario'] = df_data['Cedula Usuario'].apply(fix_dni)

    # A modo de prueba 
    #df_data = df_data.head(50)

    # Comparar valores y obtener las variables deseadas
    def get_matched_variables(row):
        cedula_usuario = row['Cedula Usuario']
        matched_report = df_binary_report[safe_contains(df_binary_report['variables'], cedula_usuario)]
    
        if len(matched_report) > 0:
            return matched_report.iloc[0][['process_id', 'nombre_del_proceso', 'numero_caso', 'nombre_del_cliente', 'generador', 'area']].to_dict()
        else:
            matched_report = df_binary_report[safe_contains(df_binary_report['cedula_socio'], cedula_usuario)]
            if len(matched_report) > 0:
                return matched_report.iloc[0][['process_id', 'nombre_del_proceso', 'numero_caso', 'nombre_del_cliente', 'generador', 'area']].to_dict()
            else:
                return {}

    df_data = df_data.progress_apply(lambda row: pd.concat([row, pd.Series(get_matched_variables(row))]), axis=1)

    # re ordenar las columnas
    df_data = df_data[['Cedula Usuario', 'Cliente TL', 'Fecha  y Hora Consumo', 'process_id', 'nombre_del_proceso', 'numero_caso', 'nombre_del_cliente', 'generador', 'area', 'RC Demografico', 'RC Biometrico']]

    return df_data
 