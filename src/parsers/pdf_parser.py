import pdfplumber
import re
import warnings

# Configuración de producción
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

def procesar_boletin_pdf(path_pdf):
    actas = set()
    regex = re.compile(r'\(21\)\s*Acta\s*[:\.]?\s*(\d+)', re.IGNORECASE)
    try:
        with pdfplumber.open(path_pdf) as pdf:
            for page in pdf.pages:
                txt = page.extract_text()
                if txt:
                    match = regex.findall(txt)
                    actas.update([int(x) for x in match])
    except: pass
    return actas, len(actas)