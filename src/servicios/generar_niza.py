import requests
import json
import time
import sys

"""
Este script genera un archivo SQL para poblar las tablas DIM_CLASES_NIZA y DIM_SUBITEMS_CLASES_NIZA
"""

# --- CONFIGURACIÓN ---
URL_API = "https://portaltramites.inpi.gob.ar/Marcas/ObtenerTerminos"
ARCHIVO_SALIDA = "poblar_niza_oficial.sql"

# --- TUS COOKIES REALES (Extraídas del Request) ---
MIS_COOKIES = "_gid=GA1.3.1433742417.1768429815; ASP.NET_SessionId=crhrvs2xdhfkqkcf101vkkjm; ASPSESSIONIDSGDBBATD=KEEFGCGDMAABLBDENNLNAGHE; ASPSESSIONIDQGADAASC=BPIKFOCAMBAAMOGCCBACEOEO; ASPSESSIONIDQEAADBTD=BACEOLPAKOJFPGCCIIMMBMDO; cookiesession1=678ADA5A97EF42231ED7CF8964ADCB9C; _gat=1; _gat_gtag_UA_113584454_1=1; ASPSESSIONIDQGAADCRD=NFOHDGMBNMPOOOLOCEFGMHBK; _ga_36T5MRQ6VW=GS2.1.s1768693928$o40$g1$t1768694933$j38$l0$h0; _ga=GA1.3.505135824.1765673603; _ga_7FZD50VFP4=GS2.3.s1768693936$o36$g1$t1768694933$j38$l0$h0"
# --- DESCRIPCIONES DE CLASES (Para poblar DIM_CLASES_NIZA) ---
CLASES_NIZA = {
    # --- PRODUCTOS ---
    1: "Químicos para industria, ciencia y agro. Resinas, plásticos en bruto, abonos y adhesivos industriales.",
    2: "Pinturas, barnices, lacas, tintas de impresión y protección contra óxido.",
    3: "Cosméticos, perfumes, jabones, limpieza del hogar, dentífricos y lociones.",
    4: "Aceites industriales, lubricantes, combustibles, velas y energía.",
    5: "Farmacéuticos, veterinarios, suplementos dietéticos, desinfectantes y material sanitario.",
    6: "Metales comunes, ferretería, cables no eléctricos, cajas fuertes y materiales de construcción metálicos.",
    7: "Máquinas industriales, motores (no vehículos), herramientas mecánicas y electrodomésticos de cocina.",
    8: "Herramientas manuales, cuchillería, tenedores, cucharas y maquinillas de afeitar.",
    9: "Software, apps, electrónica, computadoras, lentes, audio/video y aparatos científicos o de seguridad.",
    10: "Aparatos médicos, quirúrgicos, dentales, veterinarios, prótesis y ortopedia.",
    11: "Electrodomésticos de clima y cocina: iluminación, calefacción, refrigeración, hornos y sanitarios.",
    12: "Vehículos terrestres, aéreos y acuáticos. Accesorios y partes de locomoción.",
    13: "Armas de fuego, explosivos, municiones y fuegos artificiales.",
    14: "Joyería, relojes, metales preciosos y bisutería.",
    15: "Instrumentos musicales y sus accesorios.",
    16: "Papelería, material de oficina, imprenta, embalaje de papel/cartón y pinceles.",
    17: "Aislantes, selladores, gomas, plásticos semielaborados y tuberías flexibles no metálicas.",
    18: "Cuero, bolsos, carteras, maletas, paraguas y accesorios para mascotas.",
    19: "Materiales de construcción no metálicos: cemento, asfalto, madera, vidrio y tubos rígidos.",
    20: "Muebles, espejos, colchones y artículos de madera, plástico o corcho.",
    21: "Utensilios de cocina y hogar, cristalería, porcelana, cepillos y peines.",
    22: "Cuerdas, redes, carpas, lonas, sacos y materiales de relleno.",
    23: "Hilos e hilados para uso textil.",
    24: "Textiles, telas, ropa de cama, toallas y mantelería.",
    25: "Ropa, calzado y sombreros.",
    26: "Accesorios de costura: encajes, botones, cintas, bordados y flores artificiales.",
    27: "Alfombras, felpudos y revestimientos de suelos o paredes (no textiles).",
    28: "Juguetes, videojuegos, artículos de deporte y gimnasia, adornos de navidad.",
    29: "Alimentos de origen animal y procesados: carnes, lácteos, aceites, conservas de frutas/verduras.",
    30: "Alimentos vegetales básicos y dulces: café, té, harinas, pan, pastelería, especias, helados.",
    31: "Productos agrícolas frescos, frutas, verduras, semillas, plantas vivas y alimento para animales.",
    32: "Bebidas sin alcohol: aguas, jugos, gaseosas y cervezas.",
    33: "Bebidas alcohólicas: vinos, licores y espirituosas (excepto cervezas).",
    34: "Tabaco, cigarrillos electrónicos y artículos para fumadores.",

    # --- SERVICIOS ---
    35: "Publicidad, gestión de negocios, administración comercial, tiendas (retail) y venta online.",
    36: "Servicios financieros, bancarios, seguros, inmobiliarios y criptoactivos.",
    37: "Construcción, reparación, instalación y mantenimiento de equipos.",
    38: "Telecomunicaciones, transmisión de datos, acceso a internet y broadcasting.",
    39: "Transporte, logística, almacenamiento, delivery y organización de viajes.",
    40: "Tratamiento de materiales: reciclaje, impresión 3D, confección a medida y purificación.",
    41: "Educación, capacitación, entretenimiento, eventos deportivos y culturales.",
    42: "Tecnología y ciencia: desarrollo de software, ingeniería, diseño web y control de calidad.",
    43: "Gastronomía y hotelería: restaurantes, catering, bares y alojamiento temporal.",
    44: "Salud y belleza: servicios médicos, veterinarios, peluquería, spa y agricultura.",
    45: "Servicios legales, seguridad física, servicios sociales y personales."
}

def generar_script():
    print(f"🚀 Iniciando extracción con SESIÓN ACTIVA -> {ARCHIVO_SALIDA}")
    
    session = requests.Session()
    
    # Headers exactos de tu petición exitosa
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "Referer": "https://portaltramites.inpi.gob.ar/Marcas/BusquedaAntecedentes",
        "Origin": "https://portaltramites.inpi.gob.ar",
        "Content-Type": "application/json; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Cookie": MIS_COOKIES # <--- AQUÍ ESTÁ LA MAGIA
    }
    
    session.headers.update(headers)

    with open(ARCHIVO_SALIDA, "w", encoding="utf-8") as f:
        f.write("-- Script generado con datos oficiales del INPI (Sesión Clonada)\n")
        f.write("BEGIN;\n\n")
        
        # 1. Clases
        print("📝 Generando inserts de Clases...")
        f.write("-- 1. CLASES\n")
        for id_c, desc in CLASES_NIZA.items():
             f.write(f"INSERT INTO dim_clases_niza (id_clase, clase_descripcion) VALUES ({id_c}, '{desc.replace("'", "''")}') ON CONFLICT (id_clase) DO NOTHING;\n")
        f.write("\n")

        # 2. Subítems
        print("🌍 Consultando API...")
        f.write("-- 2. SUBITEMS\n")
        
        # Iniciar contador en 1000 para dejar espacio a otros si se necesita
        id_counter = 1 
        total_items = 0
        
        for clase_num in range(1, 46):
            sys.stdout.write(f"\r   ⏳ Procesando Clase {clase_num}/45... ")
            sys.stdout.flush()
            
            try:
                # Enviamos el número como string, igual que en tu request capturado
                payload = {"NumClase": str(clase_num)}
                
                response = session.post(URL_API, json=payload, timeout=10)
                
                if response.status_code == 200:
                    try:
                        items = response.json()
                    except:
                        items = [] 

                    if items:
                        f.write(f"\n-- Clase {clase_num} ({len(items)} items)\n")
                        for item in items:
                            desc_api = item.get("Descripcion", "").strip()
                            if desc_api:
                                desc_sql = desc_api.replace("'", "''")
                                sql = f"INSERT INTO dim_subitems_clases_niza (id_subitem, id_clase, subitem) VALUES ({id_counter}, {clase_num}, '{desc_sql}') ON CONFLICT DO NOTHING;"
                                f.write(sql + "\n")
                                id_counter += 1
                                total_items += 1
                    else:
                        print(f"⚠️ Vacío (¿Sesión vencida o clase vacía?)")
                else:
                    print(f"❌ Error HTTP {response.status_code}")

            except Exception as e:
                print(f"❌ Excepción: {e}")

            time.sleep(0.2) 

        print(f"\n\n✅ ¡Listo! Se extrajeron {total_items} subítems en '{ARCHIVO_SALIDA}'.")
        f.write("\nCOMMIT;\n")

if __name__ == "__main__":
    generar_script()