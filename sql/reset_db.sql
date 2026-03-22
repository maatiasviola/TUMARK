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
    marcas, dim_tipo_marca, marcas_imagenes
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
    nombre TEXT UNIQUE, 
    cuit_cuil BIGINT UNIQUE, 
    pais TEXT
);

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
    nro_acta INT REFERENCES actas(nro_acta),
    id_titular INT REFERENCES titulares(id_titular),
    porcentaje DECIMAL,
    PRIMARY KEY (nro_acta, id_titular)
);

CREATE TABLE oposiciones (
    id_oposicion SERIAL PRIMARY KEY,
    id_acta INT REFERENCES actas(id_acta),
    nro_oposicion INT,
    fecha_presentacion DATE,
    nombre_oponente TEXT,
    fundamento TEXT,
    fecha_levantamiento DATE,
    CONSTRAINT uq_oposiciones_acta_nro UNIQUE (id_acta, nro_oposicion)
);

CREATE TABLE vistas (
    id_vista SERIAL PRIMARY KEY,
    id_acta INT REFERENCES actas(id_acta), 
    id_tipo_vista INT REFERENCES dim_tipos_vistas(id_tipo_vista),
    id_oposicion INT REFERENCES oposiciones(id_oposicion), 
    fecha DATE,
    fecha_contestacion DATE, 
    fecha_vencimiento DATE
);

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