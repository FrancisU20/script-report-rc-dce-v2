import os
def to_excel(df, folder, path):
    # Si la carpeta no existe, crearla
    if not os.path.exists(folder):
        os.makedirs(folder)
    # Guardar el archivo en la carpeta
    df.to_excel(path, index=False)
    print(f"Archivo guardado en {path}")