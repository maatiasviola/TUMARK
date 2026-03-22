-- 1. TIPOS DE MARCA (Basado en tu imagen)
INSERT INTO dim_tipo_marca (id_tipo_marca, tipo_marca) VALUES
(1, 'COMBINACION DE COLORES'), (2, 'DENOMINATIVA'), (3, 'SECUENCIAL'),
(4, 'FIGURATIVA'), (5, 'GUSTATIVA'), (6, 'TACTIL'), (7, 'MIXTA'),
(8, 'OLFATIVA'), (9, 'POSICION'), (10, 'TRIDIMENSIONAL MIXTA'),
(11, 'SONORA'), (12, 'TRIDIMENSIONAL'), (13, 'DOCUMENTACION RESPALDATORIA')
ON CONFLICT (id_tipo_marca) DO NOTHING;

-- 2. ESTADOS DE TRÁMITE
INSERT INTO dim_estado_tramite_acta (id_estado_tramite, estado_tramite) VALUES
(1, 'NULIDAD ADMINISTRATIVA'), (2, 'CADUCA ADMINISTRATIVA'),
(3, 'DENEGADA'), (4, 'CONCEDIDA'), (5, 'DESISTIDA'), (6, 'EN TRAMITE')
ON CONFLICT (id_estado_tramite) DO NOTHING;

-- 3. TIPOS DE VISTA
INSERT INTO dim_tipos_vistas (id_tipo_vista, tipo_vista) VALUES
(1, 'ADMINISTRATIVAS'), (2, 'OPOSICIONES')
ON CONFLICT (id_tipo_vista) DO NOTHING;

-- Sincronizar secuencias para que los inserts automáticos sigan desde el último ID
SELECT setval('dim_tipo_marca_id_tipo_marca_seq', (SELECT MAX(id_tipo_marca) FROM dim_tipo_marca));
SELECT setval('dim_estado_tramite_acta_id_estado_tramite_seq', (SELECT MAX(id_estado_tramite) FROM dim_estado_tramite_acta));
SELECT setval('dim_tipos_vistas_id_tipo_vista_seq', (SELECT MAX(id_tipo_vista) FROM dim_tipos_vistas));