import json
import pandas as pd # type: ignore

def extract_values(dictionary,comunidad,codigo):
        b = dictionary.replace('\'', '\"').replace('None', '\"None\"').replace('False', '\"False\"').replace('True', '\"True\"').replace('false', '\"false\"').replace('true', '\"true\"')
        result = json.loads(b)

        values = result['values']
        temp = []

        for i in values:
            i['title'] = result['title']
            i['comunidad'] = comunidad
            i['codigo'] = codigo
            temp.append(i)
    
        title = result['title']
        values = temp

        return values, title

def clean_generacion(df, path):

    datos = str(path).split("_")
    comunidad = datos[3]
    codigo = datos[4].split(".")[0]

    df[['values','title']] = df['attributes'].apply(lambda x: pd.Series(extract_values(x,comunidad,codigo)))

    df_final = pd.DataFrame()
    for i in dict(df["values"]).values():
        for j in i:
            df_temp = pd.Series(j).to_frame()
            df_final = pd.concat([df_final,df_temp.transpose()])


    df_final.reset_index(inplace=True, drop=True)

    return df_final

def clean_evolucion(df, path):

    datos = str(path).split("_")
    comunidad = datos[3]
    codigo = datos[4].split(".")[0]

    df['comunidad']=comunidad
    df['codigo']=codigo

    return df
