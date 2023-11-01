import json

from java.nio.charset import StandardCharsets
from org.apache.commons.io import IOUtils
from org.apache.nifi.processor.io import OutputStreamCallback
from org.apache.nifi.processor.io import StreamCallback
from org.python.core.util import StringUtil


# Esta clase almacena los datos de los flow files en la variable output_list
class TransformCallback(StreamCallback):

    def __init__(self):
        pass

    def process(self, inputStream, outputStream):

        # Leer contenido del flow file en formato JSON
        input_text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        input_obj = json.loads(input_text)
        saved = False

        # Por cada session ya almacenada en la lista
        for output in output_list:

            # Si ya hay productos con la misma session almacenados
            if output['user_session'] == input_obj['user_session']:
                # Almacenar producto nuevo y marcar como guardado
                output['products'].append(input_obj['products'][0])
                saved = True
                break

        # Si el producto no se ha guardado
        if not saved:
            # Generar nuevo output (json) con la nueva session e introducirlo a la lista
            output_list.append({
                'user_session': input_obj['user_session'],
                'event_time': input_obj['event_time'],
                'user_id': input_obj['user_id'],
                'event_type': input_obj['event_type'],
                'products': input_obj['products']
            })

        # Escribir contenido del nuevo JSON en el flow file (los devuelve sin modificar)
        output_text = json.dumps(input_obj)
        outputStream.write(StringUtil.toBytes(output_text))


# Esta clase se encarga de intruducir los datos del JSON en un flow file
class WriteContentCallback(OutputStreamCallback):

    def __init__(self, content):
        self.content_text = content

    def process(self, outputStream):
        outputStream.write(StringUtil.toBytes(self.content_text))


# Obtener 50 flowFiles
flowFiles = session.get(50)

# Inicializar lista de sesiones (JSONs)
output_list = []

if flowFiles is not None and not flowFiles.isEmpty():

    # Por cada flow file obtenido
    for flowFile in flowFiles:
        # Almacenar los datos del flow file
        flowFile = session.write(flowFile, TransformCallback())

        # Enviar todos por el flujo de failure
        session.transfer(flowFile, REL_FAILURE)

    # Si se ha almacenado al menos una session
    if len(output_list) > 0:

        # Por cada session almacenada
        for output_json in output_list:
            # Convertir json a una string
            output_string = json.dumps(output_json)

            # Crear un nuevo flow file e introducirle datos de la session
            new_flow_file = session.create()
            new_flow_file = session.write(new_flow_file, WriteContentCallback(output_string))

            # Enviar nuevo flow file por el flujo de success
            session.transfer(new_flow_file, REL_SUCCESS)