import json
from java.nio.charset import StandardCharsets
from org.apache.commons.io import IOUtils
from org.apache.nifi.processor.io import StreamCallback
from org.python.core.util import StringUtil


# Esta clase realiza transformaciones de los flow files
class TransformCallback(StreamCallback):

    def __init__(self):
        pass

    def process(self, inputStream, outputStream):
        # Leer contenido del flow file en formato JSON
        input_text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        input_obj = json.loads(input_text)

        # Seleccionar los eventos purchase
        if input_obj['event_type'] == 'purchase':
            # Transformar contenido (cambio de estructura)
            output_obj = {
                "user_session": input_obj["user_session"],
                "event_type": input_obj["event_type"],
                "event_time": input_obj["event_time"],
                "user_id": input_obj["user_id"],
                "products": [{
                    "product_id": input_obj["product_id"],
                    "category_id": input_obj["category_id"],
                    "brand": input_obj["brand"],
                    "price": input_obj["price"]
                }]
            }

            # Escribir contenido del nuevo JSON en el flow file
            output_text = json.dumps(output_obj)
            outputStream.write(StringUtil.toBytes(output_text))


# Obtener 1 flow file
flowFile = session.get()

try:
    # Modificar y enviar por el flujo de success
    if flowFile is not None:
        flowFile = session.write(flowFile, TransformCallback())
        session.transfer(flowFile, REL_SUCCESS)

except:
    # Todos los flow files que no se hayan transferido salen por failure
    session.transfer(flowFile, REL_FAILURE)