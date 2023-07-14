from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(data, *args, **kwargs) -> DataFrame:
    """
    Execute Transformer Action: ActionType.DROP_DUPLICATE

    Docs: https://docs.mage.ai/guides/transformer-blocks#drop-duplicates
    """
    # Importar las bibliotecas necesarias
    from pymongo import MongoClient
    import pandas as pd

    # Convertir la lista en un DataFrame
    data = pd.DataFrame(data)

    # Establecer la conexión a MongoDB
    connection_string = "mongodb+srv://copito:golazo@cluster1.krfn9qj.mongodb.net/"
    client = MongoClient(connection_string)
    db = client["12"]
    collection = db["1"]

    # Crear una lista de diccionarios para los registros en el DataFrame
    records = data.to_dict(orient="records")

    # Obtener los registros duplicados en MongoDB
    duplicate_ids = []
    for record in records:
        duplicate = collection.find_one(record)
        if duplicate:
            duplicate_ids.append(duplicate["_id"])

    # Eliminar los registros duplicados en MongoDB
    collection.delete_many({"_id": {"$in": duplicate_ids}})

    # Cerrar la conexión a MongoDB
    client.close()

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'