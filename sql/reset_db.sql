-- 1. EXTENSIONES
CREATE EXTENSION IF NOT EXISTS vector;

-- 2. LIMPIEZA (DROP en orden correcto por dependencias)
DROP TABLE IF EXISTS 
    resultados_busquedas, busquedas, dim_tipo_busqueda,
    suscripciones, usuarios_titulares, usuarios, planes,
    vistas, oposiciones, dim_tipos_vistas,
    actas_titulares, titulares,
    actas_subitems_desnormalizados,
    actas_subitems, dim_subitems_clases_niza,
    actas, dim_estado_tramite_acta, dim_clases_niza, 
    marcas, dim_tipo_marca, marcas_imagenes, agentes,actas_agentes,
    boletines, actas_boletines,actas_renovaciones
CASCADE;

-- ==========================================
-- 3. TABLAS DE DIMENSIONES 
-- ==========================================

CREATE TABLE dim_clases_niza (
    id_clase INT PRIMARY KEY,
    clase_descripcion TEXT
);

CREATE TABLE dim_subitems_clases_niza (
    id_subitem INT PRIMARY KEY,
    id_clase INT REFERENCES dim_clases_niza(id_clase),
    subitem TEXT
);

CREATE TABLE dim_tipo_marca (
    id_tipo_marca SERIAL PRIMARY KEY,
    tipo_marca VARCHAR(50) UNIQUE 
);

CREATE TABLE dim_estado_tramite_acta ( 
    id_estado_tramite SERIAL PRIMARY KEY,
    estado_tramite VARCHAR(150) UNIQUE -- Modificado para match perfecto con Python
);

CREATE TABLE dim_tipos_vistas (
    id_tipo_vista SERIAL PRIMARY KEY,
    tipo_vista VARCHAR(100) UNIQUE
);

CREATE TABLE dim_tipo_busqueda (
    id_tipo_busqueda SERIAL PRIMARY KEY,
    tipo_busqueda VARCHAR(50) UNIQUE
);

CREATE TABLE planes (
    id_plan SERIAL PRIMARY KEY,
    nombre VARCHAR(50), 
    limite_busquedas INT,
    permite_descargar_pdf BOOLEAN DEFAULT FALSE,
    limite_marcas_vigilancia INT
);

-- ==========================================
-- 4. TABLAS CORE (Propiedad Intelectual)
-- ==========================================

CREATE TABLE marcas_imagenes (
    id_imagen SERIAL PRIMARY KEY,
    url_imagen TEXT, 
    embedding vector(1280), 
    hash_imagen TEXT UNIQUE -- Sugerido UNIQUE por consistencia
);

-- Índices recomendados para búsqueda vectorial
CREATE INDEX ON marcas_imagenes USING hnsw (embedding vector_cosine_ops);

CREATE TABLE titulares (
    id_titular SERIAL PRIMARY KEY,
    identidad_hash TEXT UNIQUE NOT NULL, -- Clave MDM Híbrida (Hash de CUIT o Nombre)
    cuit_cuil BIGINT,                    -- Ya NO es UNIQUE
    nombre TEXT NOT NULL,                -- Ya NO es UNIQUE
    pais TEXT,
    domicilio_real TEXT,
    localidad TEXT,
    territorio_legal TEXT
);

-- Índices recomendados para búsquedas analíticas posteriores
CREATE INDEX idx_titulares_cuit ON titulares (cuit_cuil) WHERE cuit_cuil IS NOT NULL;
CREATE INDEX idx_titulares_nombre ON titulares (nombre);


CREATE TABLE marcas (
    id_marca SERIAL PRIMARY KEY,
    id_tipo_marca INT REFERENCES dim_tipo_marca(id_tipo_marca),
    id_imagen INT REFERENCES marcas_imagenes(id_imagen),
    ids_titulares INT[],
    denominacion TEXT,
    identidad_hash BIGINT UNIQUE -- CLAVE PARA EL BULK INSERT MAGIC
);

-- TABLA ACTAS
CREATE TABLE actas (
    id_acta SERIAL PRIMARY KEY,
    nro_acta INT UNIQUE,
    id_marca INT REFERENCES marcas(id_marca),
    id_clase INT REFERENCES dim_clases_niza(id_clase),
    id_estado_tramite INT REFERENCES dim_estado_tramite_acta(id_estado_tramite),
    id_tipo_marca INT REFERENCES dim_tipo_marca(id_tipo_marca),
    id_imagen INT REFERENCES marcas_imagenes(id_imagen),
    denominacion TEXT, 
    fecha_ingreso DATE,
    fecha_vencimiento DATE,
    nro_resolucion INT,
    boletin_resolucion INT,
    fecha_resolucion DATE,
    fecha_disposicion DATE,
    es_clase_completa BOOLEAN
);

CREATE TABLE actas_subitems_desnormalizados (
    id_acta INT REFERENCES actas(id_acta),
    subitem_desnormalizado TEXT
);

CREATE TABLE actas_subitems (
    id_acta INT REFERENCES actas(id_acta), 
    id_subitem INT REFERENCES dim_subitems_clases_niza(id_subitem),
    PRIMARY KEY (id_acta, id_subitem)
);

CREATE TABLE actas_titulares (
    id_acta INT REFERENCES actas(id_acta),
    id_titular INT REFERENCES titulares(id_titular),
    porcentaje DECIMAL,
    PRIMARY KEY (id_acta, id_titular)
);

CREATE TABLE oposiciones (
    id_oposicion SERIAL PRIMARY KEY,
    id_acta INT REFERENCES actas(id_acta),
    nro_oposicion INT,
    fecha_presentacion DATE,
    nombre_oponente TEXT,
    fundamento TEXT,
    fecha_levantamiento DATE,
    CONSTRAINT uq_oposiciones_acta_nro UNIQUE NULLS NOT DISTINCT (id_acta, nro_oposicion)
);

CREATE TABLE vistas (
    id_vista SERIAL PRIMARY KEY,
    id_acta INT REFERENCES actas(id_acta), 
    id_tipo_vista INT REFERENCES dim_tipos_vistas(id_tipo_vista),
    id_oposicion INT REFERENCES oposiciones(id_oposicion), 
    fecha DATE,
    fecha_contestacion DATE, 
    fecha_vencimiento DATE,
    CONSTRAINT uq_vistas_acta UNIQUE NULLS NOT DISTINCT (id_acta, id_tipo_vista, id_oposicion, fecha)
);

CREATE TABLE agentes (
    id_agente SERIAL PRIMARY KEY,
    nro_agente INT UNIQUE,
    nombre TEXT NOT NULL
);


CREATE TABLE actas_agentes (
    id_acta INT REFERENCES actas(id_acta),
    id_agente INT REFERENCES agentes(id_agente),
    PRIMARY KEY (id_acta, id_agente)
);

CREATE TABLE boletines (
    id_boletin SERIAL PRIMARY KEY,
    nro_boletin INT UNIQUE,
    fecha_publicacion DATE
);

CREATE TABLE actas_boletines (
    id_acta INTEGER REFERENCES actas(id_acta),
    id_boletin INTEGER REFERENCES boletines(id_boletin), 
    PRIMARY KEY (id_acta, id_boletin)
);

CREATE TABLE IF NOT EXISTS actas_renovaciones (
    id_acta BIGINT REFERENCES actas(id_acta) ON DELETE CASCADE,
    nro_acta_renovada INTEGER,
    CONSTRAINT uq_acta_renovacion UNIQUE (id_acta, nro_acta_renovada)
);

-- Creamos un índice para búsquedas inversas (saber quién renovó a un acta vieja)
CREATE INDEX idx_actas_renov_nro_vieja ON actas_renovaciones(nro_acta_renovada);

-- ==========================================
-- 5. TABLAS DE USUARIOS Y APP
-- ==========================================

CREATE TABLE usuarios (
    id_usuario SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    nombre TEXT,
    apellido TEXT,
    rol VARCHAR(20) DEFAULT 'user'
);

CREATE TABLE usuarios_titulares (
    id_usuario INT REFERENCES usuarios(id_usuario),
    id_titular INT REFERENCES titulares(id_titular),
    PRIMARY KEY (id_usuario, id_titular)
);

CREATE TABLE suscripciones (
    id_usuario INT PRIMARY KEY REFERENCES usuarios(id_usuario),
    id_plan INT REFERENCES planes(id_plan),
    fecha_inicio DATE DEFAULT CURRENT_DATE,
    fecha_fin DATE,
    estado VARCHAR(20) 
);

CREATE TABLE busquedas (
    id_busqueda SERIAL PRIMARY KEY,
    id_usuario INT REFERENCES usuarios(id_usuario),
    fecha_busqueda DATE DEFAULT CURRENT_DATE,
    id_tipo_busqueda INT REFERENCES dim_tipo_busqueda(id_tipo_busqueda),
    input_text TEXT,
    input_image_url TEXT,
    filtros_buscados JSONB 
);

CREATE TABLE resultados_busquedas (
    id_busqueda INT REFERENCES busquedas(id_busqueda),
    id_marca INT REFERENCES marcas(id_marca),
    score INT,
    PRIMARY KEY (id_busqueda, id_marca)
);

-- ==========================================
-- 6. CONFIGURACIÓN DE SEGURIDAD (Backend Ingesta)
-- ==========================================
-- Apagamos RLS para permitir la inserción masiva desde los Workers
ALTER TABLE dim_clases_niza DISABLE ROW LEVEL SECURITY;
ALTER TABLE dim_subitems_clases_niza DISABLE ROW LEVEL SECURITY;
ALTER TABLE dim_tipo_marca DISABLE ROW LEVEL SECURITY;
ALTER TABLE dim_estado_tramite_acta DISABLE ROW LEVEL SECURITY;
ALTER TABLE dim_tipos_vistas DISABLE ROW LEVEL SECURITY;
ALTER TABLE dim_tipo_busqueda DISABLE ROW LEVEL SECURITY;

ALTER TABLE marcas_imagenes DISABLE ROW LEVEL SECURITY;
ALTER TABLE titulares DISABLE ROW LEVEL SECURITY;
ALTER TABLE marcas DISABLE ROW LEVEL SECURITY;
ALTER TABLE actas DISABLE ROW LEVEL SECURITY;
ALTER TABLE actas_subitems_desnormalizados DISABLE ROW LEVEL SECURITY;
ALTER TABLE actas_subitems DISABLE ROW LEVEL SECURITY;
ALTER TABLE actas_titulares DISABLE ROW LEVEL SECURITY;
ALTER TABLE oposiciones DISABLE ROW LEVEL SECURITY;
ALTER TABLE vistas DISABLE ROW LEVEL SECURITY;